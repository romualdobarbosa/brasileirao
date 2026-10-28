"""`classificacao_por_rodada`: a tabela de cada temporada rodada a rodada, só com windows."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

METRICAS = ["jogos", "vitorias", "empates", "derrotas", "gols_pro", "gols_contra"]
COLUNAS = [
    "temporada",
    "rodada",
    "clube",
    "posicao",
    "pontos",
    "ajuste_pontos",
    *METRICAS,
    "saldo",
    "aproveitamento",
    "ultimos_5",
]
# A classificação segue a rodada, não a data: a partida adiada conta na rodada dela.
ORDEM_JOGOS = ["rodada", "data", "hora", "partida_id"]
# Pontos, vitórias, saldo e gols pró decrescentes; o nome do clube desempata por último.
DESEMPATE = ["pontos", "vitorias", "saldo", "gols_pro"]


# As windows são montadas na chamada: `Window` precisa de uma SparkSession ativa.
def _por_clube() -> Window:
    return Window.partitionBy("temporada", "clube")


def _acumulado() -> Window:
    return _por_clube().orderBy("rodada").rowsBetween(Window.unboundedPreceding, Window.currentRow)


def jogos_por_clube(partidas: DataFrame) -> DataFrame:
    """Duas linhas por partida, uma na visão de cada clube, com o resultado `V`/`E`/`D`."""
    lados = [
        ("mandante", "placar_mandante", "placar_visitante"),
        ("visitante", "placar_visitante", "placar_mandante"),
    ]
    visoes = [
        partidas.select(
            "partida_id",
            "temporada",
            "rodada",
            "data",
            "hora",
            F.col(clube).alias("clube"),
            F.col(pro).alias("gols_pro"),
            F.col(contra).alias("gols_contra"),
        )
        for clube, pro, contra in lados
    ]
    jogos = visoes[0].unionByName(visoes[1])
    return jogos.withColumn(
        "resultado",
        F.when(F.col("gols_pro") > F.col("gols_contra"), "V")
        .when(F.col("gols_pro") == F.col("gols_contra"), "E")
        .otherwise("D"),
    )


def _resumo_por_rodada(jogos: DataFrame) -> DataFrame:
    """Métricas de cada clube em cada rodada em que jogou, com `ultimos_5` até ali."""
    ultimos_5 = _por_clube().orderBy(*ORDEM_JOGOS).rowsBetween(-4, Window.currentRow)
    com_ultimos = jogos.withColumn(
        "ultimos_5", F.array_join(F.collect_list("resultado").over(ultimos_5), "")
    ).withColumn("_ordem", F.row_number().over(_por_clube().orderBy(*ORDEM_JOGOS)))
    return com_ultimos.groupBy("temporada", "rodada", "clube").agg(
        F.count(F.lit(1)).alias("jogos"),
        F.count_if(F.col("resultado") == "V").alias("vitorias"),
        F.count_if(F.col("resultado") == "E").alias("empates"),
        F.count_if(F.col("resultado") == "D").alias("derrotas"),
        F.sum("gols_pro").alias("gols_pro"),
        F.sum("gols_contra").alias("gols_contra"),
        F.max_by("ultimos_5", "_ordem").alias("ultimos_5"),
    )


def _grade(jogos: DataFrame) -> DataFrame:
    """Clube × rodada: todo clube da temporada em toda rodada de 1 até a última."""
    clubes = jogos.select("temporada", "clube").distinct()
    rodadas = jogos.groupBy("temporada").agg(
        F.explode(F.sequence(F.lit(1), F.max("rodada"))).alias("rodada")
    )
    return clubes.join(rodadas, on="temporada")


def calcular_classificacao(partidas: DataFrame, ajustes: DataFrame) -> DataFrame:
    """Silver de partidas + ajustes do STJD → `classificacao_por_rodada`.

    O ajuste entra só na última rodada da temporada; as anteriores ficam com o resultado
    de campo.
    """
    jogos = jogos_por_clube(partidas)
    acumulado_ate = _acumulado()
    acumulado = (
        _grade(jogos)
        .join(_resumo_por_rodada(jogos), on=["temporada", "rodada", "clube"], how="left")
        .select(
            "temporada",
            "rodada",
            "clube",
            *[
                F.sum(F.coalesce(m, F.lit(0))).over(acumulado_ate).cast("int").alias(m)
                for m in METRICAS
            ],
            F.last("ultimos_5", ignorenulls=True).over(acumulado_ate).alias("ultimos_5"),
            F.max("rodada").over(Window.partitionBy("temporada")).alias("_ultima"),
        )
    )
    soma_ajustes = ajustes.groupBy("temporada", "clube").agg(
        F.sum("pontos").cast("int").alias("_ajuste")
    )
    campo = F.col("vitorias") * 3 + F.col("empates")
    com_pontos = (
        acumulado.join(F.broadcast(soma_ajustes), on=["temporada", "clube"], how="left")
        .withColumn(
            "ajuste_pontos",
            F.when(F.col("rodada") == F.col("_ultima"), F.coalesce("_ajuste", F.lit(0)))
            .otherwise(F.lit(0))
            .cast("int"),
        )
        .withColumn("pontos", (campo + F.col("ajuste_pontos")).cast("int"))
        .withColumn("saldo", (F.col("gols_pro") - F.col("gols_contra")).cast("int"))
        .withColumn(
            "aproveitamento",
            F.when(F.col("jogos") > 0, F.round(campo / (F.col("jogos") * 3), 4)).cast("double"),
        )
    )
    ordem = [F.desc(c) for c in DESEMPATE] + [F.asc("clube")]
    posicao = F.row_number().over(Window.partitionBy("temporada", "rodada").orderBy(*ordem))
    return com_pontos.withColumn("posicao", posicao.cast("int")).select(*COLUNAS)
