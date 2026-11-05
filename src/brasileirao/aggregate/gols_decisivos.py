"""`gols_decisivos`: gols no fim do jogo e os pontos que eles deram ou tiraram de cada clube."""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from brasileirao.aggregate.classificacao import jogos_por_clube
from brasileirao.aggregate.qualidade import LIMITE_EXEMPLOS
from brasileirao.aggregate.viradas import PRIMEIRA_TEMPORADA_GOLS

FAIXAS = ["85", "acrescimo"]
COLUNAS = [
    "temporada",
    "clube",
    "jogos",
    "gols_85",
    "gols_acrescimo",
    "pontos_ganhos_85",
    "pontos_perdidos_85",
    "pontos_ganhos_acrescimo",
    "pontos_perdidos_acrescimo",
]


def _nos_85() -> Column:
    return F.col("minuto") >= 85


def _nos_acrescimos() -> Column:
    """Só o acréscimo do fim do jogo. É um piso: nem todo gol no minuto 90 traz o acréscimo."""
    return (F.col("minuto") == 90) & F.col("acrescimo").isNotNull()


def _pontos(pro: Column, contra: Column) -> Column:
    return F.when(pro > contra, 3).when(pro == contra, 1).otherwise(0)


def _variacao_de_pontos(faixa: str) -> Column:
    """Pontos com o placar final menos pontos com o placar antes da faixa.

    Os gols do adversário saem da soma da partida. A faixa vai até o fim do jogo, então
    tirar os gols dela do placar final dá o placar antes dela.
    """
    pro, contra = F.col("gols_pro"), F.col("gols_contra")
    marcados = F.col(f"gols_{faixa}")
    sofridos = F.sum(marcados).over(Window.partitionBy("partida_id")) - marcados
    return _pontos(pro, contra) - _pontos(pro - marcados, contra - sofridos)


def calcular_gols_decisivos(gols: DataFrame, partidas: DataFrame) -> DataFrame:
    """Silver de gols e de partidas → uma linha por (`temporada`, `clube`), de 2014 em diante.

    Todo jogo do clube conta em `jogos`, mesmo sem gol na faixa ou sem gol nenhum. No gol
    contra, o `clube` já é o beneficiado.
    """
    na_faixa = gols.groupBy("partida_id", "clube").agg(
        F.count_if(_nos_85()).alias("gols_85"),
        F.count_if(_nos_acrescimos()).alias("gols_acrescimo"),
    )
    jogos = (
        jogos_por_clube(partidas.where(F.col("temporada") >= PRIMEIRA_TEMPORADA_GOLS))
        .join(na_faixa, on=["partida_id", "clube"], how="left")
        .fillna(0, subset=[f"gols_{faixa}" for faixa in FAIXAS])
    )
    for faixa in FAIXAS:
        jogos = jogos.withColumn(f"_variacao_{faixa}", _variacao_de_pontos(faixa))
    pontos = [
        F.sum(F.greatest(sinal * F.col(f"_variacao_{faixa}"), F.lit(0)))
        .cast("int")
        .alias(f"pontos_{nome}_{faixa}")
        for faixa in FAIXAS
        for nome, sinal in (("ganhos", 1), ("perdidos", -1))
    ]
    return (
        jogos.groupBy("temporada", "clube")
        .agg(
            F.count(F.lit(1)).cast("int").alias("jogos"),
            *[F.sum(f"gols_{faixa}").cast("int").alias(f"gols_{faixa}") for faixa in FAIXAS],
            *pontos,
        )
        .select(*COLUNAS)
    )


def checar_gols_decisivos(
    gols_decisivos: DataFrame, gols: DataFrame, partidas: DataFrame
) -> list[str]:
    """Temporadas em que `jogos`, `gols_85` ou `gols_acrescimo` não fecham com a silver."""
    metricas = ["jogos", "gols_85", "gols_acrescimo"]
    na_tabela = gols_decisivos.groupBy("temporada").agg(
        *[F.sum(m).alias(f"_tabela_{m}") for m in metricas]
    )
    jogos = (
        partidas.where(F.col("temporada") >= PRIMEIRA_TEMPORADA_GOLS)
        .groupBy("temporada")
        .agg((F.count(F.lit(1)) * 2).alias("_silver_jogos"))
    )
    gols_silver = gols.groupBy("temporada").agg(
        F.count_if(_nos_85()).alias("_silver_gols_85"),
        F.count_if(_nos_acrescimos()).alias("_silver_gols_acrescimo"),
    )
    silver = jogos.join(gols_silver, on="temporada", how="full").fillna(0)
    divergente = F.lit(False)
    for m in metricas:
        divergente = divergente | ~F.col(f"_tabela_{m}").eqNullSafe(F.col(f"_silver_{m}"))
    linhas = (
        na_tabela.join(silver, on="temporada", how="full")
        .where(divergente)
        .orderBy("temporada")
        .limit(LIMITE_EXEMPLOS)
        .collect()
    )
    if not linhas:
        return []
    exemplos = ", ".join(
        f"{r['temporada']}: "
        + ", ".join(
            f"{m} {r[f'_tabela_{m}'] or 0} na tabela, {r[f'_silver_{m}'] or 0} na silver"
            for m in metricas
        )
        for r in linhas
    )
    return [f"gols_decisivos: totais ≠ silver: {exemplos}"]
