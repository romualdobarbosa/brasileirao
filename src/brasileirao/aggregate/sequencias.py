"""`sequencias`: a maior série de cada tipo por clube e temporada, por gaps-and-islands."""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from brasileirao.aggregate.classificacao import jogos_por_clube

# Resultados que mantêm a série viva, por tipo.
TIPOS = {
    "invencibilidade": ["V", "E"],
    "vitorias": ["V"],
    "jejum": ["E", "D"],
    "derrotas": ["D"],
}
# As sequências seguem a data, não a rodada: a partida adiada entra quando foi jogada.
ORDEM_JOGOS = ["data", "hora", "partida_id"]


def _tipos(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [(tipo, resultado) for tipo, resultados in TIPOS.items() for resultado in resultados],
        "tipo string, resultado string",
    )


def calcular_sequencias(partidas: DataFrame) -> DataFrame:
    """Silver de partidas → uma linha por (`temporada`, `clube`, `tipo`), resultado de campo.

    A ilha de um tipo é `ordem` (posição do jogo na temporada do clube) menos a posição do
    jogo entre os que contam para o tipo: jogos seguidos do tipo têm a mesma diferença.
    """
    por_clube = Window.partitionBy("temporada", "clube")
    jogos = (
        jogos_por_clube(partidas)
        .withColumn("ordem", F.row_number().over(por_clube.orderBy(*ORDEM_JOGOS)))
        .withColumn("_total", F.count(F.lit(1)).over(por_clube))
    )
    tipos = _tipos(partidas.sparkSession)
    no_tipo = jogos.join(F.broadcast(tipos), on="resultado")
    por_tipo = Window.partitionBy("temporada", "clube", "tipo").orderBy("ordem")
    ilhas = (
        no_tipo.withColumn("ilha", F.col("ordem") - F.row_number().over(por_tipo))
        .groupBy("temporada", "clube", "tipo", "ilha")
        .agg(
            F.count(F.lit(1)).alias("jogos"),
            F.min("ordem").alias("_primeiro"),
            F.min("data").alias("data_inicio"),
            F.max("data").alias("data_fim"),
            (F.max("ordem") == F.max("_total")).cast("int").alias("em_aberto"),
        )
    )
    maior = Window.partitionBy("temporada", "clube", "tipo").orderBy(
        F.desc("jogos"), F.asc("_primeiro")
    )
    maiores = ilhas.withColumn("_n", F.row_number().over(maior)).where(F.col("_n") == 1)
    # Todo clube tem os 4 tipos: sem série do tipo, fica com 0 jogos e datas nulas.
    grade = (
        jogos.select("temporada", "clube")
        .distinct()
        .crossJoin(F.broadcast(tipos.select("tipo").distinct()))
    )
    return grade.join(maiores, on=["temporada", "clube", "tipo"], how="left").select(
        "temporada",
        "clube",
        "tipo",
        F.coalesce("jogos", F.lit(0)).cast("int").alias("jogos"),
        "data_inicio",
        "data_fim",
        F.coalesce("em_aberto", F.lit(0)).cast("int").alias("em_aberto"),
    )
