"""Raw CSV → bronze Delta. Colunas da fonte como string, sem transformação."""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from brasileirao import config
from brasileirao.utils.temporada import derivar_temporada

log = logging.getLogger(__name__)

ARQUIVOS = {
    "partidas": "campeonato-brasileiro-full.csv",
}


def ler_raw(spark: SparkSession, entidade: str) -> DataFrame:
    """CSV da fonte com todas as colunas como string, mais os metadados de ingestão."""
    caminho = f"{config.caminho_camada('raw')}/{ARQUIVOS[entidade]}"
    return (
        spark.read.option("header", True)
        .option("encoding", "UTF-8")
        .option("escape", '"')
        .csv(caminho)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


def ler_partidas_raw(spark: SparkSession) -> DataFrame:
    return ler_raw(spark, "partidas").withColumn("temporada", derivar_temporada(F.col("data")))


def contar_partidas_por_temporada(spark: SparkSession) -> dict[int, int]:
    linhas = ler_partidas_raw(spark).groupBy("temporada").count().collect()
    return {linha["temporada"]: linha["count"] for linha in linhas}


def escrever_bronze(spark: SparkSession, entidade: str, temporada: int) -> int:
    """Grava só a partição da temporada (overwrite + replaceWhere) e devolve o nº de linhas.

    Reescrever a mesma temporada substitui a partição inteira: não duplica e não toca as
    outras temporadas.
    """
    if entidade != "partidas":
        raise NotImplementedError(f"Bronze de {entidade!r} ainda não foi implementado")
    df = ler_partidas_raw(spark).where(F.col("temporada") == temporada).localCheckpoint()
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("temporada")
        .option("replaceWhere", f"temporada = {temporada}")
        .save(config.caminho_tabela("bronze", entidade))
    )
    total = df.count()
    log.info("bronze/%s temporada %d: %d linhas", entidade, temporada, total)
    return total
