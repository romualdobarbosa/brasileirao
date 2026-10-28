"""`artilharia`: gols de cada atleta por temporada, sem gol contra."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

GOL_CONTRA = "Gol Contra"


def calcular_artilharia(gols: DataFrame) -> DataFrame:
    """Silver de gols → uma linha por (`temporada`, `atleta`), com `rank()` na posição.

    O gol contra fica de fora: o `clube` dele é o beneficiado, não o do atleta.
    """
    por_atleta = (
        gols.where(~F.col("tipo_de_gol").eqNullSafe(GOL_CONTRA))
        .groupBy("temporada", "atleta")
        .agg(
            F.array_join(F.array_sort(F.collect_set("clube")), ", ").alias("clubes"),
            F.count(F.lit(1)).cast("int").alias("gols"),
            F.count_if(F.col("tipo_de_gol") == "Penalty").cast("int").alias("penaltis"),
        )
    )
    posicao = F.rank().over(Window.partitionBy("temporada").orderBy(F.desc("gols")))
    return por_atleta.select(
        "temporada",
        posicao.cast("int").alias("posicao"),
        "atleta",
        "clubes",
        "gols",
        "penaltis",
    )
