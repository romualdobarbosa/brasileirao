"""`gols_por_minuto`: gols de cada temporada por faixa de 15 minutos e por mando."""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# (faixa, último minuto). O acréscimo não muda a faixa: 45+2 fica em `31-45+`, 90+3 em `76-90+`.
FAIXAS = [
    ("0-15", 15),
    ("16-30", 30),
    ("31-45+", 45),
    ("46-60", 60),
    ("61-75", 75),
    ("76-90+", None),
]
LADOS = ["mandante", "visitante"]


def _faixa() -> Column:
    """Faixa pelo `minuto`. Minuto nulo fica sem faixa, sai na grade e a checagem acusa."""
    faixa = F.when(F.col("minuto") > FAIXAS[-2][1], FAIXAS[-1][0])
    for nome, ate in reversed(FAIXAS[:-1]):
        faixa = F.when(F.col("minuto") <= ate, nome).otherwise(faixa)
    return faixa


def calcular_gols_por_minuto(gols: DataFrame, partidas: DataFrame) -> DataFrame:
    """Silver de gols e de partidas → uma linha por (`temporada`, `faixa`, `lado`).

    O `lado` é o do clube que marcou; no gol contra, o `clube` já é o beneficiado. A grade
    tem as 12 combinações de toda temporada com gol, mesmo as que ficaram em zero.
    """
    mandantes = partidas.select("partida_id", "mandante")
    por_faixa = (
        gols.join(mandantes, on="partida_id")
        .select(
            "temporada",
            _faixa().alias("faixa"),
            F.when(F.col("clube") == F.col("mandante"), "mandante")
            .otherwise("visitante")
            .alias("lado"),
        )
        .groupBy("temporada", "faixa", "lado")
        .agg(F.count(F.lit(1)).alias("gols"))
    )
    grade = (
        gols.select("temporada")
        .distinct()
        .select("temporada", F.explode(F.array(*[F.lit(f) for f, _ in FAIXAS])).alias("faixa"))
        .select("temporada", "faixa", F.explode(F.array(*map(F.lit, LADOS))).alias("lado"))
    )
    total = F.sum("gols").over(Window.partitionBy("temporada"))
    return (
        grade.join(por_faixa, on=["temporada", "faixa", "lado"], how="left")
        .withColumn("gols", F.coalesce("gols", F.lit(0)).cast("int"))
        .withColumn("pct_gols", F.when(total > 0, F.round(F.col("gols") / total, 4)).cast("double"))
        .select("temporada", "faixa", "lado", "gols", "pct_gols")
    )
