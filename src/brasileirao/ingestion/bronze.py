"""Raw CSV → bronze Delta. Colunas da fonte como string, sem transformação."""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from brasileirao import config
from brasileirao.utils.temporada import derivar_temporada

log = logging.getLogger(__name__)

ARQUIVOS = {
    "partidas": "campeonato-brasileiro-full.csv",
    "gols": "campeonato-brasileiro-gols.csv",
    "cartoes": "campeonato-brasileiro-cartoes.csv",
    "estatisticas": "campeonato-brasileiro-estatisticas-full.csv",
}
ENTIDADES_FILHAS = ("gols", "cartoes", "estatisticas")


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


def ler_filha_raw(spark: SparkSession, entidade: str, temporada: int) -> DataFrame:
    """Linhas da filha cujas partidas estão em `bronze/partidas` na temporada.

    A filha não tem data: a temporada vem da partida. `partidas` precisa estar gravada antes.
    Linha com `partida_id` fora de `partidas` não tem temporada e fica de fora (o pipeline
    avisa as órfãs).
    """
    partidas = (
        spark.read.format("delta")
        .load(config.caminho_tabela("bronze", "partidas"))
        .where(F.col("temporada") == temporada)
        .select(F.col("ID").alias("partida_id"), "temporada")
        .distinct()
    )
    return ler_raw(spark, entidade).join(partidas, on="partida_id", how="inner")


def contar_partidas_por_temporada(spark: SparkSession) -> dict[int, int]:
    linhas = ler_partidas_raw(spark).groupBy("temporada").count().collect()
    return {linha["temporada"]: linha["count"] for linha in linhas}


def escrever_bronze(spark: SparkSession, entidade: str, temporada: int) -> int:
    """Grava só a partição da temporada (overwrite + replaceWhere) e devolve o nº de linhas.

    Reescrever a mesma temporada substitui a partição inteira: não duplica e não toca as
    outras temporadas.
    """
    if entidade == "partidas":
        df = ler_partidas_raw(spark).where(F.col("temporada") == temporada)
    elif entidade in ENTIDADES_FILHAS:
        df = ler_filha_raw(spark, entidade, temporada)
    else:
        raise ValueError(f"Entidade desconhecida: {entidade!r}. Use uma de: {', '.join(ARQUIVOS)}")
    df = df.localCheckpoint()
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
