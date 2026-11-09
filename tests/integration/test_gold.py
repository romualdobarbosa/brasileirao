import dataclasses

import pytest
from chispa import assert_df_equality

from brasileirao import config, pipeline
from brasileirao.aggregate import gold, qualidade
from brasileirao.lake import constraints
from brasileirao.serving.tabelas import TABELAS as TABELAS_SERVING


@pytest.fixture(scope="module")
def gold_uma_vez(spark, silver, lake_compartilhado):
    """O gold compartilhado das fixtures: as linhas que `processar_gold` devolveu e as tabelas
    relidas do disco, materializadas. Os testes só de leitura usam este gold; a idempotência
    roda a segunda vez por cima dele."""
    linhas = lake_compartilhado.linhas_gold
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
        "artilharia": 7,
        "gols_por_minuto": 12,
        "resumo_temporada": 1,
        "confrontos": 12,
        "goleadas": 0,
        "campanhas": 4,
        "linha_corte": 1,
        "passagens_tecnicos": 0,
        "esquemas_taticos": 0,
        "viradas": 1,
        "gols_decisivos": 4,
        "cartoes_jogadores": 2,
        "faltas_clubes": 2,
    }


def test_colunas_batem_com_as_tabelas_do_serving(gold_uma_vez):
    """O DDL do Postgres (`serving/tabelas.py`) acompanha o schema que o gold grava."""
    _, tabelas = gold_uma_vez

    for tabela in TABELAS_SERVING:
        assert tabela.colunas == tuple(tabelas[tabela.nome].columns), tabela.nome


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
    assert tabelas["gols_por_minuto"].groupBy().sum("gols").first()[0] == 21
    resumo = tabelas["resumo_temporada"].first()
    assert (resumo["campeao"], resumo["artilheiro"], resumo["gols_artilheiro"]) == (
        "Flamengo",
        "Arrascaeta, Gabriel",
        5,
    )
    # São Paulo saiu na frente aos 5' da partida 5, e o Flamengo virou para 3 × 1.
    virada = tabelas["viradas"].first()
    assert (virada["partida_id"], virada["clube"], virada["maior_desvantagem"]) == (
        5,
        "Flamengo",
        1,
    )
    # O empate do Flamengo aos 90+3 da partida 9: o Santos perde 2 pontos e o Flamengo ganha 1.
    decisivos = {r["clube"]: r for r in tabelas["gols_decisivos"].collect()}
    assert decisivos["Flamengo"]["pontos_ganhos_acrescimo"] == 1
    assert decisivos["Santos"]["pontos_perdidos_acrescimo"] == 2
    campanhas = {r["clube"]: r["situacao"] for r in tabelas["campanhas"].collect()}
    assert campanhas == {
        "Flamengo": "campeao",
        "Palmeiras": "vice",
        "Santos": "permaneceu",
        "São Paulo": "rebaixado",
    }


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
    # Só o despacho: as constraints de verdade são testadas em `test_constraints`. Sem o
    # dublê, elas seriam aplicadas no gold do lake compartilhado, que é de outros módulos.
    chamadas = []
    monkeypatch.setattr(gold, "processar_gold", lambda sessao: chamadas.append(sessao))
    monkeypatch.setattr(
        constraints, "aplicar_camada", lambda sessao, camada: chamadas.append((sessao, camada))
    )

    pipeline.main(["gold"])

    assert chamadas == [spark, (spark, "gold")]


def test_entrada_desconhecida(spark):
    with pytest.raises(KeyError, match="bronze/partidas"):
        gold.Fontes(spark)["bronze/partidas"]
