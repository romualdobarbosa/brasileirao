"""`passagens_tecnicos`: as passagens de cada técnico por clube e temporada (gaps-and-islands)."""

import csv
import io
from importlib import resources

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from brasileirao.aggregate.classificacao import jogos_por_clube
from brasileirao.aggregate.qualidade import LIMITE_EXEMPLOS

# 2025 fica de fora: a fonte repete nessa temporada o técnico do clube na hora da coleta em quase
# todos os jogos (Tite aparece com 36 jogos pelo Cruzeiro, que foi dirigido por Leonardo Jardim).
ULTIMA_TEMPORADA = 2024
SCHEMA_SEED = "nome_fonte string, clube string, temporada int, nome_popular string"
# As passagens seguem a data, não a rodada: a partida adiada entra quando foi jogada.
ORDEM_JOGOS = ["data", "hora", "partida_id"]
COLUNAS = [
    "temporada",
    "clube",
    "ordem",
    "tecnico",
    "data_inicio",
    "data_fim",
    "jogos",
    "vitorias",
    "empates",
    "derrotas",
    "gols_pro",
    "gols_contra",
    "pontos",
    "aproveitamento",
]


def carregar_seed_tecnicos(spark: SparkSession) -> DataFrame:
    """`reference/tecnicos.csv`: nome da fonte (e clube e temporada, se preciso) → nome popular."""
    texto = resources.files("brasileirao.reference").joinpath("tecnicos.csv").read_text("utf-8")
    linhas = [
        (
            r["nome_fonte"],
            r["clube"] or None,
            int(r["temporada"]) if r["temporada"] else None,
            r["nome_popular"],
        )
        for r in csv.DictReader(io.StringIO(texto))
    ]
    return spark.createDataFrame(linhas, SCHEMA_SEED)


def jogos_com_tecnico(partidas: DataFrame) -> DataFrame:
    """`jogos_por_clube` com o técnico como veio da fonte, até `ULTIMA_TEMPORADA` e sem os jogos
    de técnico nulo."""
    ate_o_corte = partidas.where(F.col("temporada") <= ULTIMA_TEMPORADA)
    tecnicos = ate_o_corte.select("partida_id", "tecnico_mandante", "tecnico_visitante")
    return (
        jogos_por_clube(ate_o_corte)
        .join(tecnicos, on="partida_id")
        .withColumn(
            "tecnico",
            F.when(F.col("mando") == "mandante", F.col("tecnico_mandante")).otherwise(
                F.col("tecnico_visitante")
            ),
        )
        .where(F.col("tecnico").isNotNull())
        .drop("tecnico_mandante", "tecnico_visitante")
    )


def padronizar_tecnicos(jogos: DataFrame, seed: DataFrame) -> DataFrame:
    """Troca o nome da fonte pelo do seed. Vale a linha mais específica: clube e temporada,
    depois só clube, depois a geral. Sem seed, fica o nome da fonte."""
    fonte = F.col("nome_fonte").alias("tecnico")
    por_temporada = seed.where(F.col("clube").isNotNull() & F.col("temporada").isNotNull()).select(
        fonte, "clube", "temporada", F.col("nome_popular").alias("_da_temporada")
    )
    por_clube = seed.where(F.col("clube").isNotNull() & F.col("temporada").isNull()).select(
        fonte, "clube", F.col("nome_popular").alias("_do_clube")
    )
    geral = seed.where(F.col("clube").isNull() & F.col("temporada").isNull()).select(
        fonte, F.col("nome_popular").alias("_geral")
    )
    return (
        jogos.join(F.broadcast(por_temporada), on=["tecnico", "clube", "temporada"], how="left")
        .join(F.broadcast(por_clube), on=["tecnico", "clube"], how="left")
        .join(F.broadcast(geral), on="tecnico", how="left")
        .withColumn("tecnico", F.coalesce("_da_temporada", "_do_clube", "_geral", "tecnico"))
        .select(*jogos.columns)
    )


def calcular_passagens_tecnicos(partidas: DataFrame, seed: DataFrame) -> DataFrame:
    """Silver de partidas + seed → uma linha por passagem, resultado de campo.

    A ilha é a posição do jogo no clube menos a posição dele entre os jogos do técnico: jogos
    seguidos do mesmo técnico têm a mesma diferença, e quem sai e volta cai em outra ilha. O
    jogo de técnico nulo sai antes de numerar, então não interrompe a passagem.
    """
    jogos = padronizar_tecnicos(jogos_com_tecnico(partidas), seed)
    por_clube = Window.partitionBy("temporada", "clube").orderBy(*ORDEM_JOGOS)
    por_tecnico = Window.partitionBy("temporada", "clube", "tecnico").orderBy(*ORDEM_JOGOS)
    ilhas = (
        jogos.withColumn("_pos", F.row_number().over(por_clube))
        .withColumn("_ilha", F.col("_pos") - F.row_number().over(por_tecnico))
        .groupBy("temporada", "clube", "tecnico", "_ilha")
        .agg(
            F.min("_pos").alias("_primeiro"),
            F.min("data").alias("data_inicio"),
            F.max("data").alias("data_fim"),
            F.count(F.lit(1)).cast("int").alias("jogos"),
            F.count_if(F.col("resultado") == "V").cast("int").alias("vitorias"),
            F.count_if(F.col("resultado") == "E").cast("int").alias("empates"),
            F.count_if(F.col("resultado") == "D").cast("int").alias("derrotas"),
            F.sum("gols_pro").cast("int").alias("gols_pro"),
            F.sum("gols_contra").cast("int").alias("gols_contra"),
        )
    )
    pontos = F.col("vitorias") * 3 + F.col("empates")
    ordem = Window.partitionBy("temporada", "clube").orderBy("_primeiro")
    return (
        ilhas.withColumn("ordem", F.row_number().over(ordem).cast("int"))
        .withColumn("pontos", pontos.cast("int"))
        .withColumn("aproveitamento", F.round(pontos / (F.col("jogos") * 3), 4).cast("double"))
        .select(*COLUNAS)
    )


def checar_passagens_tecnicos(passagens: DataFrame, partidas: DataFrame) -> list[str]:
    """Temporadas em que os jogos das passagens diferem dos jogos de clube com técnico."""
    na_tabela = passagens.groupBy("temporada").agg(F.sum("jogos").alias("tabela"))
    na_silver = (
        jogos_com_tecnico(partidas).groupBy("temporada").agg(F.count(F.lit(1)).alias("silver"))
    )
    linhas = (
        na_tabela.join(na_silver, on="temporada", how="full")
        .where(~F.col("tabela").eqNullSafe(F.col("silver")))
        .orderBy("temporada")
        .limit(LIMITE_EXEMPLOS)
        .collect()
    )
    if not linhas:
        return []
    exemplos = ", ".join(
        f"{r['temporada']}: {r['tabela'] or 0} na tabela, {r['silver'] or 0} na silver"
        for r in linhas
    )
    return [f"passagens_tecnicos: jogos ≠ jogos de clube com técnico: {exemplos}"]
