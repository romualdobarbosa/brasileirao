from pyspark.sql import functions as F

from brasileirao import config
from brasileirao.ingestion import bronze


def _bronze(spark):
    return spark.read.format("delta").load(config.caminho_tabela("bronze", "partidas"))


def test_grava_so_a_temporada_pedida(spark, raw):
    total = bronze.escrever_bronze(spark, "partidas", 2019)

    df = _bronze(spark)
    assert total == 4
    assert [r["temporada"] for r in df.select("temporada").distinct().collect()] == [2019]


def test_colunas_da_fonte_como_string_e_metadados(spark, raw):
    bronze.escrever_bronze(spark, "partidas", 2003)

    tipos = dict(_bronze(spark).dtypes)
    assert tipos.pop("_ingested_at") == "timestamp"
    assert tipos.pop("temporada") == "int"
    assert set(tipos.values()) == {"string"}
    assert "rodata" in tipos and "ID" in tipos
    arquivo = _bronze(spark).select("_source_file").first()[0]
    assert arquivo.endswith("raw/campeonato-brasileiro-full.csv")


def test_bronze_nao_transforma_o_valor(spark, raw):
    bronze.escrever_bronze(spark, "partidas", 2003)

    arenas = {r["ID"]: r["arena"] for r in _bronze(spark).collect()}
    assert arenas["1"] == " Brinco de Ouro"


def test_jan_e_fev_de_2021_vao_para_2020(spark, raw):
    bronze.escrever_bronze(spark, "partidas", 2020)
    bronze.escrever_bronze(spark, "partidas", 2021)

    temporadas = {r["ID"]: r["temporada"] for r in _bronze(spark).collect()}
    assert temporadas == {"20": 2020, "21": 2020, "22": 2020, "30": 2021}


def test_reescrever_nao_duplica_nem_toca_outras_temporadas(spark, raw):
    bronze.escrever_bronze(spark, "partidas", 2019)
    bronze.escrever_bronze(spark, "partidas", 2020)
    antes_2020 = _bronze(spark).where(F.col("temporada") == 2020).collect()

    bronze.escrever_bronze(spark, "partidas", 2019)

    df = _bronze(spark)
    assert df.where(F.col("temporada") == 2019).count() == 4
    assert df.where(F.col("temporada") == 2020).collect() == antes_2020


def test_contagem_por_temporada(spark, raw):
    assert bronze.contar_partidas_por_temporada(spark) == {
        2003: 2,
        2019: 4,
        2020: 3,
        2021: 1,
        2025: 1,
    }
