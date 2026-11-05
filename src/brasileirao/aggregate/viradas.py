"""`viradas`: jogos que o clube venceu depois de estar atrás, pelo placar corrente gol a gol."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from brasileirao.aggregate.classificacao import jogos_por_clube
from brasileirao.aggregate.qualidade import LIMITE_EXEMPLOS

# `silver/gols` começa aqui. Partida anterior tem placar, mas não tem gols para reconstruir.
PRIMEIRA_TEMPORADA_GOLS = 2014
COLUNAS = [
    "partida_id",
    "clube",
    "temporada",
    "rodada",
    "data",
    "adversario",
    "mando",
    "gols_pro",
    "gols_contra",
    "maior_desvantagem",
]


def placar_corrente(gols: DataFrame, partidas: DataFrame) -> DataFrame:
    """Uma linha por gol, com `corrente_mandante` e `corrente_visitante` logo depois dele.

    A ordem é `minuto` e depois o acréscimo, com 45+2 antes do 46. O frame padrão, de range,
    soma de uma vez os gols do mesmo instante: não há como saber qual veio antes, e assim o
    resultado não depende da ordem das linhas. No gol contra, o `clube` já é o beneficiado.
    """
    ordem = Window.partitionBy("partida_id").orderBy(
        F.col("minuto"), F.coalesce(F.col("acrescimo"), F.lit(0))
    )
    lados = partidas.select("partida_id", "mandante", "visitante")
    return (
        gols.join(lados, on="partida_id")
        .withColumn(
            "corrente_mandante",
            F.sum((F.col("clube") == F.col("mandante")).cast("int")).over(ordem).cast("int"),
        )
        .withColumn(
            "corrente_visitante",
            F.sum((F.col("clube") == F.col("visitante")).cast("int")).over(ordem).cast("int"),
        )
    )


def calcular_viradas(gols: DataFrame, partidas: DataFrame) -> DataFrame:
    """Silver de gols e de partidas → uma linha por (`partida_id`, `clube`), o clube que virou."""
    desvantagens = (
        placar_corrente(gols, partidas)
        .groupBy("partida_id")
        .agg(
            F.max(F.col("corrente_visitante") - F.col("corrente_mandante")).alias("_do_mandante"),
            F.max(F.col("corrente_mandante") - F.col("corrente_visitante")).alias("_do_visitante"),
        )
    )
    lados = partidas.select("partida_id", "mandante", "visitante")
    eh_mandante = F.col("mando") == "mandante"
    return (
        jogos_por_clube(partidas)
        .where(F.col("resultado") == "V")
        .join(lados, on="partida_id")
        .join(desvantagens, on="partida_id")
        .withColumn(
            "adversario", F.when(eh_mandante, F.col("visitante")).otherwise(F.col("mandante"))
        )
        .withColumn(
            "maior_desvantagem",
            F.when(eh_mandante, F.col("_do_mandante")).otherwise(F.col("_do_visitante")),
        )
        .where(F.col("maior_desvantagem") > 0)
        .select(*COLUNAS)
    )


def _placares_divergentes(gols: DataFrame, partidas: DataFrame) -> list[str]:
    """Partidas de 2014 em diante em que o placar reconstruído difere do placar da partida.

    Gol de partida que não está em `partidas` também aparece aqui. O placar corrente só
    cresce, então o máximo de cada partida é o placar da última linha.
    """
    reconstruido = (
        placar_corrente(gols, partidas)
        .groupBy("partida_id")
        .agg(
            F.max("corrente_mandante").alias("_mandante"),
            F.max("corrente_visitante").alias("_visitante"),
        )
    )
    orfaos = gols.join(partidas.select("partida_id"), on="partida_id", how="left_anti")
    placares = partidas.where(F.col("temporada") >= PRIMEIRA_TEMPORADA_GOLS).select(
        "partida_id", "placar_mandante", "placar_visitante"
    )
    linhas = (
        placares.join(reconstruido, on="partida_id", how="left")
        .where(
            (F.coalesce(F.col("_mandante"), F.lit(0)) != F.col("placar_mandante"))
            | (F.coalesce(F.col("_visitante"), F.lit(0)) != F.col("placar_visitante"))
        )
        .select("partida_id")
        .unionByName(orfaos.select("partida_id").distinct())
        .orderBy("partida_id")
        .limit(LIMITE_EXEMPLOS)
        .collect()
    )
    return [str(r["partida_id"]) for r in linhas]


def checar_viradas(viradas: DataFrame, gols: DataFrame, partidas: DataFrame) -> list[str]:
    """Placar reconstruído diferente do placar da partida, ou linha que não é virada."""
    invalidas = (
        viradas.where(
            (F.col("gols_pro") <= F.col("gols_contra"))
            | (F.col("maior_desvantagem") < 1)
            | (F.col("maior_desvantagem") > F.col("gols_contra"))
        )
        .orderBy("partida_id", "clube")
        .limit(LIMITE_EXEMPLOS)
        .collect()
    )
    checagens = [
        ("placar reconstruído ≠ placar da partida", _placares_divergentes(gols, partidas)),
        ("linha que não é virada", [f"{r['partida_id']} {r['clube']}" for r in invalidas]),
    ]
    return [f"viradas: {titulo}: {', '.join(ex)}" for titulo, ex in checagens if ex]
