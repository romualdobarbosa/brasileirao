"""`faltas_clubes`: faltas de cada clube por temporada, nos jogos que têm o dado."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from brasileirao.aggregate.qualidade import LIMITE_EXEMPLOS

# 2014 tem só 32 linhas em `silver/estatisticas`: não dá para comparar com as outras.
PRIMEIRA_TEMPORADA = 2015


def _com_dado(estatisticas: DataFrame) -> DataFrame:
    return estatisticas.where(
        (F.col("temporada") >= PRIMEIRA_TEMPORADA) & F.col("faltas").isNotNull()
    )


def calcular_faltas_clubes(estatisticas: DataFrame) -> DataFrame:
    """Silver de estatísticas → uma linha por (`temporada`, `clube`), de 2015 em diante.

    A média divide pelos jogos com dado, não pelos jogos do clube: 2019 tem 714 de 760. Uma
    temporada sem nenhum dado, como 2024, não gera linha.
    """
    return (
        _com_dado(estatisticas)
        .groupBy("temporada", "clube")
        .agg(
            F.count(F.lit(1)).cast("int").alias("jogos_com_dado"),
            F.sum("faltas").cast("int").alias("faltas"),
        )
        .withColumn(
            "faltas_por_jogo",
            F.round(F.col("faltas") / F.col("jogos_com_dado"), 2).cast("double"),
        )
    )


def checar_faltas_clubes(faltas_clubes: DataFrame, estatisticas: DataFrame) -> list[str]:
    """Temporadas em que `jogos_com_dado` ou `faltas` não fecham com a silver."""
    na_tabela = faltas_clubes.groupBy("temporada").agg(
        F.sum("jogos_com_dado").alias("_jogos_tabela"),
        F.sum("faltas").alias("_faltas_tabela"),
    )
    na_silver = (
        _com_dado(estatisticas)
        .groupBy("temporada")
        .agg(
            F.count(F.lit(1)).alias("_jogos_silver"),
            F.sum("faltas").alias("_faltas_silver"),
        )
    )
    linhas = (
        na_tabela.join(na_silver, on="temporada", how="full")
        .where(
            ~F.col("_jogos_tabela").eqNullSafe(F.col("_jogos_silver"))
            | ~F.col("_faltas_tabela").eqNullSafe(F.col("_faltas_silver"))
        )
        .orderBy("temporada")
        .limit(LIMITE_EXEMPLOS)
        .collect()
    )
    if not linhas:
        return []
    exemplos = ", ".join(
        f"{r['temporada']}: {r['_jogos_tabela'] or 0} jogos e {r['_faltas_tabela'] or 0} faltas "
        f"na tabela, {r['_jogos_silver'] or 0} jogos e {r['_faltas_silver'] or 0} na silver"
        for r in linhas
    )
    return [f"faltas_clubes: totais ≠ silver/estatisticas: {exemplos}"]
