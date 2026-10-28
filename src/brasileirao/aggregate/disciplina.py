"""`disciplina`: cartões por clube e temporada, da silver de cartões ou das estatísticas."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from brasileirao.aggregate.classificacao import jogos_por_clube

COLUNAS = [
    "temporada",
    "clube",
    "jogos",
    "amarelos",
    "vermelhos",
    "amarelos_por_jogo",
    "vermelhos_por_jogo",
    "fonte",
]


def _por_jogo(coluna: str) -> Column:
    return F.when(F.col("jogos") > 0, F.round(F.col(coluna) / F.col("jogos"), 2)).cast("double")


def calcular_disciplina(
    partidas: DataFrame, cartoes: DataFrame, estatisticas: DataFrame
) -> DataFrame:
    """Silver de partidas, cartões e estatísticas → uma linha por (`temporada`, `clube`).

    Temporada com alguma linha em `cartoes` usa `cartoes`; sem nenhuma, usa as somas de
    `estatisticas`; sem as duas, fica de fora. Clube com cartão e sem partida na temporada
    entra com `jogos = 0`, para o aviso da qualidade achar.
    """
    de_cartoes = cartoes.groupBy("temporada", "clube").agg(
        F.count_if(F.col("cartao") == "Amarelo").alias("amarelos"),
        F.count_if(F.col("cartao") == "Vermelho").alias("vermelhos"),
    )
    com_cartoes = cartoes.select("temporada").distinct()
    de_estatisticas = (
        estatisticas.join(com_cartoes, on="temporada", how="left_anti")
        .groupBy("temporada", "clube")
        .agg(
            F.sum(F.coalesce("cartao_amarelo", F.lit(0))).alias("amarelos"),
            F.sum(F.coalesce("cartao_vermelho", F.lit(0))).alias("vermelhos"),
        )
    )
    contagens = de_cartoes.withColumn("fonte", F.lit("cartoes")).unionByName(
        de_estatisticas.withColumn("fonte", F.lit("estatisticas"))
    )
    # Uma linha por temporada: pequena o bastante para ir em broadcast nos joins.
    fonte = F.broadcast(contagens.select("temporada", "fonte").distinct())
    jogos = (
        jogos_por_clube(partidas)
        .groupBy("temporada", "clube")
        .agg(F.count(F.lit(1)).alias("jogos"))
        .join(fonte, on="temporada")
    )
    return (
        jogos.join(contagens, on=["temporada", "clube", "fonte"], how="full")
        .select(
            "temporada",
            "clube",
            *[
                F.coalesce(c, F.lit(0)).cast("int").alias(c)
                for c in ("jogos", "amarelos", "vermelhos")
            ],
            "fonte",
        )
        .withColumn("amarelos_por_jogo", _por_jogo("amarelos"))
        .withColumn("vermelhos_por_jogo", _por_jogo("vermelhos"))
        .select(*COLUNAS)
    )
