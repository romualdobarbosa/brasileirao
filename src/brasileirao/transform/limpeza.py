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


COLUNAS_GOLS = [
    "partida_id",
    "temporada",
    "rodada",
    "clube",
    "atleta",
    "minuto",
    "acrescimo",
    "tipo_de_gol",
]
COLUNAS_CARTOES = [
    "partida_id",
    "temporada",
    "rodada",
    "clube",
    "cartao",
    "atleta",
    "num_camisa",
    "posicao",
    "minuto",
    "acrescimo",
]
COLUNAS_INTEIRAS_ESTATISTICAS = [
    "chutes",
    "chutes_no_alvo",
    "passes",
    "faltas",
    "cartao_amarelo",
    "cartao_vermelho",
    "impedimentos",
    "escanteios",
]
COLUNAS_PERCENTUAIS_ESTATISTICAS = ["posse_de_bola", "precisao_passes"]
COLUNAS_ESTATISTICAS = [
    "partida_id",
    "temporada",
    "rodada",
    "clube",
    "chutes",
    "chutes_no_alvo",
    "posse_de_bola",
    "passes",
    "precisao_passes",
    "faltas",
    "cartao_amarelo",
    "cartao_vermelho",
    "impedimentos",
    "escanteios",
]


def _chave_da_partida() -> list[Column]:
    """Colunas comuns às filhas: id, temporada, rodada e clube (ainda com o nome da fonte)."""
    return [
        _texto("partida_id").try_cast("int").alias("partida_id"),
        F.col("temporada").cast("int"),
        _texto("rodata").try_cast("int").alias("rodada"),
        _texto("clube").alias("clube"),
    ]


def _minuto_e_acrescimo(coluna: str) -> list[Column]:
    """`"45+2"` vira minuto 45 e acréscimo 2. Sem `+`, o acréscimo fica nulo."""
    partes = F.split(_aparar(_texto(coluna)), r"\+")
    return [
        F.try_element_at(partes, F.lit(1)).try_cast("int").alias("minuto"),
        F.try_element_at(partes, F.lit(2)).try_cast("int").alias("acrescimo"),
    ]


def limpar_gols(bronze: DataFrame) -> DataFrame:
    """Renomeia e tipa. `tipo_de_gol` vazio (gol normal) vira nulo."""
    return bronze.select(
        *_chave_da_partida(),
        _texto("atleta").alias("atleta"),
        *_minuto_e_acrescimo("minuto"),
        _texto("tipo_de_gol").alias("tipo_de_gol"),
    )


def limpar_cartoes(bronze: DataFrame) -> DataFrame:
    """Renomeia e tipa. A posição "Zagueira" vira "Zagueiro"; vazia vira nulo."""
    posicao = _aparar(_texto("posicao"))
    return bronze.select(
        *_chave_da_partida(),
        _texto("cartao").alias("cartao"),
        _texto("atleta").alias("atleta"),
        _texto("num_camisa").try_cast("int").alias("num_camisa"),
        F.when(posicao == "Zagueira", "Zagueiro").otherwise(posicao).alias("posicao"),
        *_minuto_e_acrescimo("minuto"),
    )


def _percentual(coluna: str) -> Column:
    """`"45%"` vira 0.45; vazio e o texto `"None"` viram nulo."""
    valor = _aparar(_texto(coluna))
    numero = F.regexp_replace(valor, "%$", "").try_cast("double") / 100
    return F.when(valor == "None", None).otherwise(numero).alias(coluna)


def limpar_estatisticas(bronze: DataFrame) -> DataFrame:
    """Renomeia e tipa. Não descarta nada: o descarte é `descartar_estatisticas_sem_dado`."""
    return bronze.select(
        *_chave_da_partida(),
        *[_texto(c).try_cast("int").alias(c) for c in COLUNAS_INTEIRAS_ESTATISTICAS],
        *[_percentual(c) for c in COLUNAS_PERCENTUAIS_ESTATISTICAS],
    ).select(*COLUNAS_ESTATISTICAS)


def descartar_estatisticas_sem_dado(estatisticas: DataFrame) -> DataFrame:
    """Tira as linhas que a fonte preenche com zero onde não há estatística (antes de 2014
    e 2024 inteira): `chutes`, `passes` e `posse_de_bola` todos zerados ou nulos."""
    sem_dado = F.lit(True)
    for coluna in ("chutes", "passes", "posse_de_bola"):
        sem_dado = sem_dado & F.coalesce(F.col(coluna) == 0, F.lit(True))
    return estatisticas.where(~sem_dado)


def para_silver_filha(limpo: DataFrame, mapa: DataFrame, colunas: list[str]) -> DataFrame:
    """Filha limpa → schema da silver: clube canônico e sem linhas idênticas."""
    return normalizar_times(limpo, mapa, ["clube"]).select(*colunas).dropDuplicates()
