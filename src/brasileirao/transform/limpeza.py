"""Transformações puras DataFrame → DataFrame, sem leitura nem escrita."""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from brasileirao.utils.temporada import FORMATO_DATA

COLUNAS_TIME_PARTIDAS = ["mandante", "visitante", "vencedor"]
COLUNAS_PARTIDAS = [
    "partida_id",
    "temporada",
    "rodada",
    "data",
    "hora",
    "mandante",
    "visitante",
    "placar_mandante",
    "placar_visitante",
    "vencedor",
    "arena",
    "uf_mandante",
    "uf_visitante",
    "tecnico_mandante",
    "tecnico_visitante",
    "formacao_mandante",
    "formacao_visitante",
    "arrecadacao",
]
TIPO_ARRECADACAO = "decimal(14,2)"


# `F.trim` só tira o espaço ASCII; a arena da fonte vem com espaço não separável (U+00A0).
ESPACO_NAS_PONTAS = r"^[\s\u00a0]+|[\s\u00a0]+$"


def _aparar(coluna: Column) -> Column:
    return F.regexp_replace(coluna, ESPACO_NAS_PONTAS, "")


def _texto(coluna: str) -> Column:
    """String vazia (ou só espaços) vira nulo."""
    return F.when(_aparar(F.col(coluna)) == "", None).otherwise(F.col(coluna))


def limpar_partidas(bronze: DataFrame) -> DataFrame:
    """Renomeia e tipa as colunas da fonte. Times continuam com o nome da fonte.

    Valor não numérico vira nulo (`try_cast`), para a qualidade apontar a partida em vez
    de a execução quebrar no meio do cast.
    """
    return bronze.select(
        _texto("ID").try_cast("int").alias("partida_id"),
        F.col("temporada").cast("int"),
        _texto("rodata").try_cast("int").alias("rodada"),
        F.to_date(_texto("data"), FORMATO_DATA).alias("data"),
        _texto("hora").alias("hora"),
        _texto("mandante").alias("mandante"),
        _texto("visitante").alias("visitante"),
        _texto("mandante_Placar").try_cast("int").alias("placar_mandante"),
        _texto("visitante_Placar").try_cast("int").alias("placar_visitante"),
        F.when(_aparar(F.col("vencedor")) == "-", None)
        .otherwise(_texto("vencedor"))
        .alias("vencedor"),
        _aparar(_texto("arena")).alias("arena"),
        _texto("mandante_Estado").alias("uf_mandante"),
        _texto("visitante_Estado").alias("uf_visitante"),
        _texto("tecnico_mandante").alias("tecnico_mandante"),
        _texto("tecnico_visitante").alias("tecnico_visitante"),
        _texto("formacao_mandante").alias("formacao_mandante"),
        _texto("formacao_visitante").alias("formacao_visitante"),
        _texto("arrecadacao").try_cast(TIPO_ARRECADACAO).alias("arrecadacao"),
        F.col("_ingested_at"),
    )


def deduplicar_partidas(partidas: DataFrame) -> DataFrame:
    """Uma linha por `partida_id`: fica a de `_ingested_at` mais recente.

    O desempate pelo conteúdo da linha deixa o resultado determinístico, o que a carga
    idempotente precisa. Linhas com `partida_id` nulo passam direto para a qualidade pegar.
    """
    conteudo = [F.col(c).desc_nulls_last() for c in partidas.columns if c != "_ingested_at"]
    janela = Window.partitionBy("partida_id").orderBy(F.col("_ingested_at").desc(), *conteudo)
    return (
        partidas.withColumn("_ordem", F.row_number().over(janela))
        .where((F.col("_ordem") == 1) | F.col("partida_id").isNull())
        .drop("_ordem")
    )


def normalizar_times(partidas: DataFrame, mapa: DataFrame, colunas: list[str]) -> DataFrame:
    """Troca cada nome da fonte pelo canônico.

    Nome sem mapeamento vira nulo e é pego pela qualidade.
    """
    for coluna in colunas:
        canonico = mapa.select(
            F.col("nome_fonte").alias(coluna),
            F.col("nome_canonico").alias(f"{coluna}_canonico"),
        )
        partidas = (
            partidas.join(F.broadcast(canonico), on=coluna, how="left")
            .drop(coluna)
            .withColumnRenamed(f"{coluna}_canonico", coluna)
        )
    return partidas


def para_silver_partidas(limpo: DataFrame, mapa: DataFrame) -> DataFrame:
    """Partidas limpas e deduplicadas → schema de `silver/partidas`, com times canônicos."""
    return normalizar_times(limpo, mapa, COLUNAS_TIME_PARTIDAS).select(*COLUNAS_PARTIDAS)
