"""`desempenho_mandante_visitante`: campanha de cada clube em casa e fora, sem ajustes."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from brasileirao.aggregate.classificacao import jogos_por_clube

METRICAS = ["jogos", "vitorias", "empates", "derrotas", "gols_pro", "gols_contra", "pontos"]


def calcular_desempenho(partidas: DataFrame) -> DataFrame:
    """Silver de partidas → uma linha por (`temporada`, `clube`, `mando`), resultado de campo."""
    por_mando = (
        jogos_por_clube(partidas)
        .groupBy("temporada", "clube", "mando")
        .agg(
            F.count(F.lit(1)).alias("jogos"),
            F.count_if(F.col("resultado") == "V").alias("vitorias"),
            F.count_if(F.col("resultado") == "E").alias("empates"),
            F.count_if(F.col("resultado") == "D").alias("derrotas"),
            F.sum("gols_pro").alias("gols_pro"),
            F.sum("gols_contra").alias("gols_contra"),
        )
    )
    return por_mando.withColumn("pontos", F.col("vitorias") * 3 + F.col("empates")).select(
        "temporada",
        "clube",
        "mando",
        *[F.col(m).cast("int").alias(m) for m in METRICAS],
        F.round(F.col("pontos") / (F.col("jogos") * 3), 4).cast("double").alias("aproveitamento"),
    )
