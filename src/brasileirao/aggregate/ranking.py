"""`ranking_historico`: a soma das campanhas finais de cada clube em todas as temporadas."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from brasileirao.aggregate.classificacao import DESEMPATE
from brasileirao.aggregate.qualidade import ultima_rodada

SOMADAS = ["jogos", "vitorias", "empates", "derrotas", "gols_pro", "gols_contra", "saldo"]
COLUNAS = [
    "posicao",
    "clube",
    "temporadas",
    "primeira_temporada",
    "ultima_temporada",
    *SOMADAS,
    "pontos",
    "aproveitamento",
    "titulos",
]


def calcular_ranking(classificacao: DataFrame) -> DataFrame:
    """`classificacao_por_rodada` → uma linha por clube, com a última rodada de cada temporada.

    Os pontos incluem os ajustes do STJD; o `aproveitamento` é só de campo.
    """
    por_clube = (
        ultima_rodada(classificacao)
        .groupBy("clube")
        .agg(
            F.count(F.lit(1)).alias("temporadas"),
            F.min("temporada").alias("primeira_temporada"),
            F.max("temporada").alias("ultima_temporada"),
            *[F.sum(m).alias(m) for m in SOMADAS],
            F.sum("pontos").alias("pontos"),
            F.sum(F.col("pontos") - F.col("ajuste_pontos")).alias("_campo"),
            F.count_if(F.col("posicao") == 1).alias("titulos"),
        )
        .withColumn(
            "aproveitamento",
            F.when(F.col("jogos") > 0, F.round(F.col("_campo") / (F.col("jogos") * 3), 4)).cast(
                "double"
            ),
        )
    )
    ordem = [F.desc(c) for c in DESEMPATE] + [F.asc("clube")]
    # Uma partição só, com uma linha por clube da história (dezenas): o sort global é barato.
    posicao = F.row_number().over(Window.orderBy(*ordem))
    return por_clube.withColumn("posicao", posicao).select(
        *[
            F.col(c) if c in ("clube", "aproveitamento") else F.col(c).cast("int").alias(c)
            for c in COLUNAS
        ]
    )
