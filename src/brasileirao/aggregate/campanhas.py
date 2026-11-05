"""`campanhas`: a tabela final de cada temporada, com a situação de cada clube."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from brasileirao.aggregate.qualidade import LIMITE_EXEMPLOS, ultima_rodada
from brasileirao.utils.temporada import rebaixados_por_temporada

COLUNAS = [
    "temporada",
    "clube",
    "posicao",
    "pontos",
    "ajuste_pontos",
    "jogos",
    "vitorias",
    "empates",
    "derrotas",
    "gols_pro",
    "gols_contra",
    "saldo",
    "aproveitamento",
    "situacao",
]


def calcular_campanhas(classificacao: DataFrame) -> DataFrame:
    """`classificacao_por_rodada` → uma linha por (`temporada`, `clube`), na última rodada.

    Os pontos têm os ajustes do STJD; o aproveitamento é de campo, como na classificação.
    """
    final = ultima_rodada(classificacao).withColumn(
        "_times", F.count(F.lit(1)).over(Window.partitionBy("temporada"))
    )
    # Uma linha por temporada (duas dezenas): a regra de `utils` vira um mapa literal.
    temporadas = [r["temporada"] for r in final.select("temporada").distinct().collect()]
    rebaixados = F.create_map(
        *[F.lit(v) for t in temporadas for v in (t, rebaixados_por_temporada(t))]
    )
    situacao = (
        F.when(F.col("posicao") == 1, "campeao")
        .when(F.col("posicao") == 2, "vice")
        .when(F.col("posicao") > F.col("_times") - rebaixados[F.col("temporada")], "rebaixado")
        .otherwise("permaneceu")
    )
    return final.withColumn("situacao", situacao).select(*COLUNAS)


def checar_campanhas(campanhas: DataFrame, resumo: DataFrame) -> list[str]:
    """Temporadas em que campeão, vice ou rebaixados diferem de `resumo_temporada`."""
    rebaixados = F.array_join(
        F.transform(
            F.array_sort(
                F.collect_list(
                    F.when(
                        F.col("situacao") == "rebaixado",
                        F.struct("posicao", F.col("clube").alias("nome")),
                    )
                )
            ),
            lambda s: s["nome"],
        ),
        ", ",
    )
    na_tabela = campanhas.groupBy("temporada").agg(
        F.max(F.when(F.col("situacao") == "campeao", F.col("clube"))).alias("_campeao"),
        F.max(F.when(F.col("situacao") == "vice", F.col("clube"))).alias("_vice"),
        rebaixados.alias("_rebaixados"),
    )
    linhas = (
        na_tabela.join(resumo, on="temporada", how="full")
        .where(
            ~F.col("_campeao").eqNullSafe(F.col("campeao"))
            | ~F.col("_vice").eqNullSafe(F.col("vice"))
            | ~F.col("_rebaixados").eqNullSafe(F.col("rebaixados"))
        )
        .orderBy("temporada")
        .limit(LIMITE_EXEMPLOS)
        .collect()
    )
    if not linhas:
        return []
    exemplos = ", ".join(str(r["temporada"]) for r in linhas)
    return [f"campanhas: situação difere de resumo_temporada em: {exemplos}"]
