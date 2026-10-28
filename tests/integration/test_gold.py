import dataclasses
import shutil
from pathlib import Path

import pytest
from chispa import assert_df_equality

from brasileirao import config, pipeline
from brasileirao.aggregate import gold, qualidade

FIXTURES_RAW_GOLD = Path(__file__).parent.parent / "fixtures" / "raw_gold"


@pytest.fixture(scope="module")
def silver(spark, tmp_path_factory):
    """Silver das fixtures `raw_gold` (o minicampeonato em 2019), montada uma vez por módulo."""
    raiz = tmp_path_factory.mktemp("lake")
    shutil.copytree(FIXTURES_RAW_GOLD, raiz / "raw")
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("BRASILEIRAO_DATA_ROOT", str(raiz))
        assert pipeline.lakehouse(spark, None) == [2019]
        yield raiz


@pytest.fixture(scope="module")
def gold_uma_vez(spark, silver):
    """O gold das fixtures gravado uma vez: as linhas que `processar_gold` devolveu e as
    tabelas relidas do disco, materializadas. Os testes só de leitura usam este gold; a
    idempotência roda a segunda vez por cima dele."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("BRASILEIRAO_DATA_ROOT", str(silver))
        shutil.rmtree(silver / "gold", ignore_errors=True)
        linhas = gold.processar_gold(spark)
        tabelas = {nome: df.localCheckpoint() for nome, df in _ler_gold(spark).items()}
    return linhas, tabelas


@pytest.fixture
def lake(silver, monkeypatch):
    monkeypatch.setenv("BRASILEIRAO_DATA_ROOT", str(silver))
    return silver


def _ler_gold(spark):
    return {
        nome: spark.read.format("delta").load(config.caminho_tabela("gold", nome))
        for nome in gold.TABELAS
    }


def _arquivos(raiz):
    return sorted((str(p), p.stat().st_mtime_ns) for p in raiz.rglob("*") if p.is_file())


def test_linhas_de_cada_tabela(gold_uma_vez):
    linhas, _ = gold_uma_vez

    assert linhas == {
        "classificacao_por_rodada": 24,
        "desempenho_mandante_visitante": 8,
        "sequencias": 16,
        "ranking_historico": 4,
        "disciplina": 5,
        "artilharia": 2,
        "gols_por_minuto": 12,
        "resumo_temporada": 1,
    }


def test_rodar_duas_vezes_e_igual_a_uma(spark, lake, gold_uma_vez, caplog):
    _, uma_vez = gold_uma_vez

    gold.processar_gold(spark)

    for nome, df in _ler_gold(spark).items():
        assert_df_equality(df, uma_vez[nome], ignore_row_order=True)
    assert "disciplina: clube sem partida na temporada: 2019 Grêmio" in caplog.text


def test_classificacao_final_das_fixtures(gold_uma_vez):
    _, tabelas = gold_uma_vez

    final = tabelas["classificacao_por_rodada"].where("rodada = 6").orderBy("posicao").collect()
    assert [(r["clube"], r["pontos"]) for r in final] == [
        ("Flamengo", 14),
        ("Palmeiras", 7),
        ("Santos", 7),
        ("São Paulo", 3),
    ]


def test_tabelas_de_eventos_das_fixtures(gold_uma_vez):
    _, tabelas = gold_uma_vez

    disciplina = {
        r["clube"]: (r["jogos"], r["amarelos"], r["fonte"]) for r in tabelas["disciplina"].collect()
    }
    # O Grêmio levou cartão sem jogar: entra com 0 jogos e gera o aviso.
    assert disciplina == {
        "Flamengo": (6, 0, "cartoes"),
        "Palmeiras": (6, 1, "cartoes"),
        "Santos": (6, 0, "cartoes"),
        "São Paulo": (6, 0, "cartoes"),
        "Grêmio": (0, 1, "cartoes"),
    }
    assert tabelas["gols_por_minuto"].groupBy().sum("gols").first()[0] == 2
    resumo = tabelas["resumo_temporada"].first()
    assert (resumo["campeao"], resumo["artilheiro"], resumo["gols_artilheiro"]) == (
        "Flamengo",
        "Arrascaeta, Gabriel",
        1,
    )


def test_checagem_que_falha_nao_grava_nada(spark, lake, monkeypatch):
    reprovada = dataclasses.replace(
        gold.TABELAS["classificacao_por_rodada"], checar=lambda *_: ["problema forçado"]
    )
    # Só a tabela reprovada no registro: o resto não muda o teste e custaria tempo.
    monkeypatch.setattr(gold, "TABELAS", {"classificacao_por_rodada": reprovada})
    antes = _arquivos(lake / "gold")

    with pytest.raises(qualidade.QualidadeGoldError, match="problema forçado"):
        gold.processar_gold(spark)

    assert _arquivos(lake / "gold") == antes


def test_sem_silver_falha_com_mensagem(spark, data_root):
    with pytest.raises(gold.SilverAusenteError, match="make lakehouse"):
        gold.processar_gold(spark)


def test_cli_gold_sem_silver(spark, data_root):
    with pytest.raises(SystemExit) as saida:
        pipeline.main(["gold"])

    assert "make lakehouse" in str(saida.value.code)


def test_cli_gold(spark, monkeypatch):
    chamadas = []
    monkeypatch.setattr(gold, "processar_gold", lambda sessao: chamadas.append(sessao))

    pipeline.main(["gold"])

    assert chamadas == [spark]


def test_entrada_desconhecida(spark):
    with pytest.raises(KeyError, match="bronze/partidas"):
        gold.Fontes(spark)["bronze/partidas"]
