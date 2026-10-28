"""`resumo_temporada`: uma linha por temporada com totais, pódio, rebaixados e artilheiro."""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from brasileirao.aggregate.qualidade import ultima_rodada
from brasileirao.utils.temporada import rebaixados_por_temporada

COLUNAS = [
    "temporada",
    "times",
    "partidas",
    "gols",
    "media_gols",
    "pct_vitoria_mandante",
    "pct_empate",
    "pct_vitoria_visitante",
    "campeao",
    "vice",
    "rebaixados",
    "artilheiro",
    "gols_artilheiro",
]


def _nomes(clubes_ordenados: Column) -> Column:
    """Array de structs ordenável → nomes separados por `", "`, na ordem do array."""
    return F.array_join(F.transform(F.array_sort(clubes_ordenados), lambda s: s["nome"]), ", ")


def _totais(partidas: DataFrame) -> DataFrame:
    mandante, visitante = F.col("placar_mandante"), F.col("placar_visitante")
    pct = {
        "pct_vitoria_mandante": mandante > visitante,
        "pct_empate": mandante == visitante,
        "pct_vitoria_visitante": mandante < visitante,
    }
    return (
        partidas.groupBy("temporada")
        .agg(
            F.count(F.lit(1)).alias("partidas"),
            F.sum(mandante + visitante).alias("gols"),
            *[F.count_if(cond).alias(nome) for nome, cond in pct.items()],
        )
        .select(
            "temporada",
            F.col("partidas").cast("int"),
            F.col("gols").cast("int"),
            F.round(F.col("gols") / F.col("partidas"), 2).cast("double").alias("media_gols"),
            *[F.round(F.col(c) / F.col("partidas"), 4).cast("double").alias(c) for c in pct],
        )
    )


def _podio(classificacao: DataFrame) -> DataFrame:
    """Campeão, vice e rebaixados da última rodada, com os ajustes do STJD."""
    final = ultima_rodada(classificacao).withColumn(
        "_times", F.count(F.lit(1)).over(Window.partitionBy("temporada"))
    )
    # Uma linha por temporada (duas dezenas): a regra de `utils` vira um mapa literal.
    temporadas = [r["temporada"] for r in final.select("temporada").distinct().collect()]
    rebaixados = F.create_map(
        *[F.lit(v) for t in temporadas for v in (t, rebaixados_por_temporada(t))]
    )
    caiu = F.col("posicao") > F.col("_times") - rebaixados[F.col("temporada")]
    return final.groupBy("temporada").agg(
        F.count(F.lit(1)).cast("int").alias("times"),
        F.max(F.when(F.col("posicao") == 1, F.col("clube"))).alias("campeao"),
        F.max(F.when(F.col("posicao") == 2, F.col("clube"))).alias("vice"),
        _nomes(
            F.collect_list(F.when(caiu, F.struct("posicao", F.col("clube").alias("nome"))))
        ).alias("rebaixados"),
    )


def _artilheiros(artilharia: DataFrame) -> DataFrame:
    return (
        artilharia.where(F.col("posicao") == 1)
        .groupBy("temporada")
        .agg(
            _nomes(F.collect_list(F.struct(F.col("atleta").alias("nome")))).alias("artilheiro"),
            F.max("gols").cast("int").alias("gols_artilheiro"),
        )
    )


def calcular_resumo(
    partidas: DataFrame, classificacao: DataFrame, artilharia: DataFrame
) -> DataFrame:
    """Silver de partidas + `classificacao_por_rodada` + `artilharia` → uma linha por
    temporada. Sem gols na silver (antes de 2014), o artilheiro fica nulo."""
    return (
        _totais(partidas)
        .join(_podio(classificacao), on="temporada")
        .join(_artilheiros(artilharia), on="temporada", how="left")
        .select(*COLUNAS)
    )
