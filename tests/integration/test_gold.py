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
    """Silver das fixtures `raw_gold` (o minicampeonato em 2019), montada uma vez por módulo.

    Cada teste apaga o gold que encontra, para não depender da ordem.
    """
    raiz = tmp_path_factory.mktemp("lake")
    shutil.copytree(FIXTURES_RAW_GOLD, raiz / "raw")
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("BRASILEIRAO_DATA_ROOT", str(raiz))
        assert pipeline.lakehouse(spark, None) == [2019]
        yield raiz


@pytest.fixture
def lake(silver, monkeypatch):
    monkeypatch.setenv("BRASILEIRAO_DATA_ROOT", str(silver))
    shutil.rmtree(silver / "gold", ignore_errors=True)
    return silver


def _ler_gold(spark):
    return {
        nome: spark.read.format("delta").load(config.caminho_tabela("gold", nome))
        for nome in gold.TABELAS
    }


def test_rodar_duas_vezes_e_igual_a_uma(spark, lake):
    linhas = gold.processar_gold(spark)
    uma_vez = {nome: df.localCheckpoint() for nome, df in _ler_gold(spark).items()}

    gold.processar_gold(spark)

    assert linhas == {
        "classificacao_por_rodada": 24,
        "desempenho_mandante_visitante": 8,
        "sequencias": 16,
        "ranking_historico": 4,
    }
    for nome, df in _ler_gold(spark).items():
        assert_df_equality(df, uma_vez[nome], ignore_row_order=True)


def test_classificacao_final_das_fixtures(spark, lake):
    gold.processar_gold(spark)

    final = (
        _ler_gold(spark)["classificacao_por_rodada"]
        .where("rodada = 6")
        .orderBy("posicao")
        .collect()
    )
    assert [(r["clube"], r["pontos"]) for r in final] == [
        ("Flamengo", 14),
        ("Palmeiras", 7),
        ("Santos", 7),
        ("São Paulo", 3),
    ]


def test_checagem_que_falha_nao_grava_nada(spark, lake, monkeypatch):
    reprovada = dataclasses.replace(
        gold.TABELAS["classificacao_por_rodada"], checar=lambda *_: ["problema forçado"]
    )
    monkeypatch.setitem(gold.TABELAS, "classificacao_por_rodada", reprovada)

    with pytest.raises(qualidade.QualidadeGoldError, match="problema forçado"):
        gold.processar_gold(spark)

    assert not (lake / "gold").exists()


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
