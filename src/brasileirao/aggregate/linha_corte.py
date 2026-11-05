"""`linha_corte`: quantos pontos salvaram e quantos rebaixaram em cada temporada.

As temporadas têm de 38 a 46 rodadas: o aproveitamento é o que permite comparar.
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from brasileirao.aggregate.qualidade import LIMITE_EXEMPLOS

COLUNAS = [
    "temporada",
    "times",
    "rebaixados",
    "ultimo_salvo",
    "pontos_ultimo_salvo",
    "aproveitamento_ultimo_salvo",
    "primeiro_rebaixado",
    "pontos_primeiro_rebaixado",
    "aproveitamento_primeiro_rebaixado",
    "pontos_campeao",
    "pontos_vice",
]


def calcular_linha_corte(campanhas: DataFrame) -> DataFrame:
    """`campanhas` → uma linha por temporada. O último salvo é quem fica logo acima do primeiro
    rebaixado; o campeão e o vice saem da mesma tabela."""
    temporada = Window.partitionBy("temporada")
    posicionado = (
        campanhas.withColumn("_times", F.count(F.lit(1)).over(temporada))
        .withColumn(
            "_rebaixados", F.sum((F.col("situacao") == "rebaixado").cast("int")).over(temporada)
        )
        .withColumn("_corte", F.col("_times") - F.col("_rebaixados"))
    )
    salvo = F.col("posicao") == F.col("_corte")
    caiu = F.col("posicao") == F.col("_corte") + 1

    def valor(coluna: str, condicao: Column) -> Column:
        return F.max(F.when(condicao, F.col(coluna)))

    return (
        posicionado.groupBy("temporada")
        .agg(
            F.first("_times").cast("int").alias("times"),
            F.first("_rebaixados").cast("int").alias("rebaixados"),
            valor("clube", salvo).alias("ultimo_salvo"),
            valor("pontos", salvo).cast("int").alias("pontos_ultimo_salvo"),
            valor("aproveitamento", salvo).cast("double").alias("aproveitamento_ultimo_salvo"),
            valor("clube", caiu).alias("primeiro_rebaixado"),
            valor("pontos", caiu).cast("int").alias("pontos_primeiro_rebaixado"),
            valor("aproveitamento", caiu).cast("double").alias("aproveitamento_primeiro_rebaixado"),
            valor("pontos", F.col("posicao") == 1).cast("int").alias("pontos_campeao"),
            valor("pontos", F.col("posicao") == 2).cast("int").alias("pontos_vice"),
        )
        .select(*COLUNAS)
    )


def checar_linha_corte(linha_corte: DataFrame, campanhas: DataFrame) -> list[str]:
    """Temporada faltando, ou com a ordem da tabela invertida entre os pontos da linha."""
    problemas = []
    temporadas = campanhas.select("temporada").distinct()
    faltando = temporadas.join(linha_corte, on="temporada", how="left_anti").collect()
    if faltando:
        exemplos = ", ".join(sorted(str(r["temporada"]) for r in faltando))
        problemas.append(f"linha_corte: temporada faltando: {exemplos}")
    invertidas = (
        linha_corte.where(
            (F.col("pontos_ultimo_salvo") < F.col("pontos_primeiro_rebaixado"))
            | (F.col("pontos_campeao") < F.col("pontos_vice"))
        )
        .orderBy("temporada")
        .limit(LIMITE_EXEMPLOS)
        .collect()
    )
    if invertidas:
        exemplos = ", ".join(str(r["temporada"]) for r in invertidas)
        problemas.append(f"linha_corte: pontos fora de ordem em: {exemplos}")
    return problemas
