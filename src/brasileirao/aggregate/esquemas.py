"""`esquemas_taticos`: uso e rendimento de cada esquema por temporada, a partir de 2015."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from brasileirao.aggregate.classificacao import jogos_por_clube
from brasileirao.aggregate.qualidade import LIMITE_EXEMPLOS

# 2014 tem só 16 partidas com formação na fonte.
PRIMEIRA_TEMPORADA = 2015
# Cada `uso_pct` arredonda em até 0,00005; com 22 esquemas, a soma erra no máximo 0,0011.
TOLERANCIA_USO = 0.002
COLUNAS = [
    "temporada",
    "esquema",
    "jogos",
    "uso_pct",
    "vitorias",
    "empates",
    "derrotas",
    "aproveitamento",
]


def calcular_esquemas_taticos(partidas: DataFrame) -> DataFrame:
    """Silver de partidas → uma linha por (`temporada`, `esquema`). Um jogo conta duas vezes,
    uma para cada clube; quem não tem formação informada fica fora, também de `uso_pct`."""
    a_partir = partidas.where(F.col("temporada") >= PRIMEIRA_TEMPORADA)
    formacoes = a_partir.select("partida_id", "formacao_mandante", "formacao_visitante")
    jogos = (
        jogos_por_clube(a_partir)
        .join(formacoes, on="partida_id")
        .withColumn(
            "esquema",
            F.when(F.col("mando") == "mandante", F.col("formacao_mandante")).otherwise(
                F.col("formacao_visitante")
            ),
        )
        .where(F.col("esquema").isNotNull())
    )
    totais = jogos.groupBy("temporada", "esquema").agg(
        F.count(F.lit(1)).cast("int").alias("jogos"),
        F.count_if(F.col("resultado") == "V").cast("int").alias("vitorias"),
        F.count_if(F.col("resultado") == "E").cast("int").alias("empates"),
        F.count_if(F.col("resultado") == "D").cast("int").alias("derrotas"),
    )
    pontos = F.col("vitorias") * 3 + F.col("empates")
    jogos_da_temporada = F.sum("jogos").over(Window.partitionBy("temporada"))
    return (
        totais.withColumn("uso_pct", F.round(F.col("jogos") / jogos_da_temporada, 4).cast("double"))
        .withColumn("aproveitamento", F.round(pontos / (F.col("jogos") * 3), 4).cast("double"))
        .select(*COLUNAS)
    )


def checar_esquemas_taticos(esquemas: DataFrame) -> list[str]:
    """Temporada anterior a 2015 ou com `uso_pct` que não soma 1."""
    problemas = []
    antigas = esquemas.where(F.col("temporada") < PRIMEIRA_TEMPORADA).limit(1).count()
    if antigas:
        problemas.append(f"esquemas_taticos: temporada anterior a {PRIMEIRA_TEMPORADA}")
    linhas = (
        esquemas.groupBy("temporada")
        .agg(F.sum("uso_pct").alias("soma"))
        .where(F.abs(F.col("soma") - 1) > TOLERANCIA_USO)
        .orderBy("temporada")
        .limit(LIMITE_EXEMPLOS)
        .collect()
    )
    if linhas:
        exemplos = ", ".join(f"{r['temporada']}: {r['soma']:.4f}" for r in linhas)
        problemas.append(f"esquemas_taticos: uso_pct não soma 1: {exemplos}")
    return problemas
