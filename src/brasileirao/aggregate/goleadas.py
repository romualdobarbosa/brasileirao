"""`goleadas`: as partidas de diferença de 4 gols ou mais, ou de 8 gols ou mais no total."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from brasileirao.aggregate.qualidade import LIMITE_EXEMPLOS

DIFERENCA_MINIMA = 4
TOTAL_MINIMO = 8
COLUNAS = [
    "partida_id",
    "temporada",
    "rodada",
    "data",
    "mandante",
    "visitante",
    "placar_mandante",
    "placar_visitante",
    "diferenca",
    "total_gols",
    "arena",
]


def _eh_goleada(diferenca: Column, total_gols: Column) -> Column:
    return (diferenca >= DIFERENCA_MINIMA) | (total_gols >= TOTAL_MINIMO)


def calcular_goleadas(partidas: DataFrame) -> DataFrame:
    """Silver de partidas → uma linha por partida que entra no critério."""
    return (
        partidas.withColumn(
            "diferenca", F.abs(F.col("placar_mandante") - F.col("placar_visitante")).cast("int")
        )
        .withColumn(
            "total_gols", (F.col("placar_mandante") + F.col("placar_visitante")).cast("int")
        )
        .where(_eh_goleada(F.col("diferenca"), F.col("total_gols")))
        .select(*COLUNAS)
    )


def checar_goleadas(goleadas: DataFrame) -> list[str]:
    """Partida repetida ou que não atende ao critério."""
    problemas = []
    repetidas = (
        goleadas.groupBy("partida_id")
        .count()
        .where(F.col("count") > 1)
        .orderBy("partida_id")
        .limit(LIMITE_EXEMPLOS)
        .collect()
    )
    if repetidas:
        exemplos = ", ".join(str(r["partida_id"]) for r in repetidas)
        problemas.append(f"goleadas: partida repetida: {exemplos}")
    fora = (
        goleadas.where(~_eh_goleada(F.col("diferenca"), F.col("total_gols")))
        .orderBy("partida_id")
        .limit(LIMITE_EXEMPLOS)
        .collect()
    )
    if fora:
        exemplos = ", ".join(str(r["partida_id"]) for r in fora)
        problemas.append(f"goleadas: partida fora do critério: {exemplos}")
    return problemas
