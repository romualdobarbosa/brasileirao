import pytest
from chispa import assert_df_equality

from brasileirao import config, pipeline

METADADOS_INGESTAO = ["_ingested_at", "_source_file"]


def _ler(spark, camada, tabela):
    return spark.read.format("delta").load(config.caminho_tabela(camada, tabela))


def _estado(spark):
    """Conteúdo das tabelas sem os metadados de ingestão, que mudam a cada execução."""
    return {
        "bronze/partidas": _ler(spark, "bronze", "partidas").drop(*METADADOS_INGESTAO),
        "silver/partidas": _ler(spark, "silver", "partidas"),
    }


def test_rodar_duas_vezes_e_igual_a_uma(spark, raw):
    pipeline.lakehouse(spark, 2019)
    uma_vez = {nome: df.localCheckpoint() for nome, df in _estado(spark).items()}

    pipeline.lakehouse(spark, 2019)

    for nome, df in _estado(spark).items():
        assert_df_equality(df, uma_vez[nome], ignore_row_order=True)


def test_todas_em_ordem_e_sem_a_temporada_em_andamento(spark, raw):
    assert pipeline.lakehouse(spark, None) == [2003, 2019, 2020, 2021]

    temporadas = {r["temporada"] for r in _ler(spark, "silver", "partidas").collect()}
    assert temporadas == {2003, 2019, 2020, 2021}
    assert _ler(spark, "bronze", "partidas").where("temporada = 2025").isEmpty()


def test_cli_todas(spark, raw):
    pipeline.main(["lakehouse", "--todas"])

    assert _ler(spark, "silver", "partidas").count() == 9


def test_cli_recusa_temporada_em_andamento(spark, raw):
    with pytest.raises(SystemExit) as saida:
        pipeline.main(["lakehouse", "--temporada", "2025"])

    assert "2025 está em andamento (1 de 380 partidas)" in str(saida.value.code)
    assert not (raw.parent / "bronze").exists()


def test_cli_exige_temporada_ou_todas():
    with pytest.raises(SystemExit) as saida:
        pipeline.main(["lakehouse"])

    assert saida.value.code == 2
