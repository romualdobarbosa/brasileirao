"""`confrontos`: o retrospecto de cada clube contra cada adversário, 2003 em diante."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from brasileirao.aggregate.classificacao import jogos_por_clube
from brasileirao.aggregate.qualidade import LIMITE_EXEMPLOS

COLUNAS = [
    "clube",
    "adversario",
    "jogos",
    "vitorias",
    "empates",
    "derrotas",
    "gols_pro",
    "gols_contra",
    "aproveitamento",
    "maior_vitoria",
    "maior_derrota",
    "data_ultimo_jogo",
    "ultimo_jogo",
]
PAR = ["clube", "adversario"]


def _jogos_do_par(partidas: DataFrame) -> DataFrame:
    """`jogos_por_clube` com o adversário e o texto do jogo, sempre mandante primeiro."""
    mandante, visitante = F.col("mandante"), F.col("visitante")
    return (
        jogos_por_clube(partidas)
        .join(
            partidas.select(
                "partida_id", "mandante", "visitante", "placar_mandante", "placar_visitante"
            ),
            on="partida_id",
        )
        .withColumn(
            "adversario", F.when(F.col("mando") == "mandante", visitante).otherwise(mandante)
        )
        .withColumn(
            "_jogo",
            F.format_string(
                "%s %d × %d %s", mandante, "placar_mandante", "placar_visitante", visitante
            ),
        )
    )


def _maior(jogos: DataFrame, resultado: str, coluna: str) -> DataFrame:
    """O jogo de maior saldo com o `resultado`, por par. Desempata por mais gols marcados e
    depois pelo mais recente. Com o saldo igual, mais gols pró é também mais gols contra:
    a maior derrota de um é o espelho da maior vitória do outro."""
    ordem = Window.partitionBy(*PAR).orderBy(
        F.desc(F.abs(F.col("gols_pro") - F.col("gols_contra"))),
        F.desc("gols_pro"),
        F.desc("data"),
        F.desc("hora"),
        F.desc("partida_id"),
    )
    return (
        jogos.where(F.col("resultado") == resultado)
        .withColumn("_n", F.row_number().over(ordem))
        .where(F.col("_n") == 1)
        .select(
            *PAR,
            F.format_string("%d × %d (%d)", "gols_pro", "gols_contra", "temporada").alias(coluna),
        )
    )


def calcular_confrontos(partidas: DataFrame) -> DataFrame:
    """Silver de partidas → duas linhas por par de clubes, uma na visão de cada um."""
    jogos = _jogos_do_par(partidas)
    momento = F.struct("data", "hora", "partida_id")
    totais = jogos.groupBy(*PAR).agg(
        F.count(F.lit(1)).cast("int").alias("jogos"),
        F.count_if(F.col("resultado") == "V").cast("int").alias("vitorias"),
        F.count_if(F.col("resultado") == "E").cast("int").alias("empates"),
        F.count_if(F.col("resultado") == "D").cast("int").alias("derrotas"),
        F.sum("gols_pro").cast("int").alias("gols_pro"),
        F.sum("gols_contra").cast("int").alias("gols_contra"),
        F.max_by("data", momento).alias("data_ultimo_jogo"),
        F.max_by("_jogo", momento).alias("ultimo_jogo"),
    )
    pontos = F.col("vitorias") * 3 + F.col("empates")
    return (
        totais.withColumn(
            "aproveitamento", F.round(pontos / (F.col("jogos") * 3), 4).cast("double")
        )
        .join(_maior(jogos, "V", "maior_vitoria"), on=PAR, how="left")
        .join(_maior(jogos, "D", "maior_derrota"), on=PAR, how="left")
        .select(*COLUNAS)
    )


def checar_confrontos(confrontos: DataFrame, partidas: DataFrame) -> list[str]:
    """Jogos que não somam 2 × partidas, ou par sem o espelho igual do outro lado."""
    problemas = []
    jogos = confrontos.agg(F.sum("jogos")).first()[0] or 0
    esperado = partidas.count() * 2
    if jogos != esperado:
        problemas.append(f"confrontos: {jogos} jogos, esperado 2 × partidas = {esperado}")
    espelho = confrontos.select(
        F.col("adversario").alias("clube"),
        F.col("clube").alias("adversario"),
        F.col("jogos").alias("_jogos"),
        F.col("vitorias").alias("_derrotas"),
        F.col("derrotas").alias("_vitorias"),
        F.col("gols_pro").alias("_gols_contra"),
        F.col("gols_contra").alias("_gols_pro"),
    )
    assimetricos = (
        confrontos.join(espelho, on=PAR, how="full")
        .where(
            ~F.col("jogos").eqNullSafe(F.col("_jogos"))
            | ~F.col("vitorias").eqNullSafe(F.col("_vitorias"))
            | ~F.col("derrotas").eqNullSafe(F.col("_derrotas"))
            | ~F.col("gols_pro").eqNullSafe(F.col("_gols_pro"))
            | ~F.col("gols_contra").eqNullSafe(F.col("_gols_contra"))
        )
        .orderBy(*PAR)
        .limit(LIMITE_EXEMPLOS)
        .collect()
    )
    if assimetricos:
        exemplos = ", ".join(f"{r['clube']} × {r['adversario']}" for r in assimetricos)
        problemas.append(f"confrontos: par sem espelho: {exemplos}")
    return problemas
