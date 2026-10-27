"""Checagens da silver. Rodam antes da escrita: uma falha não deixa a tabela pela metade."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from brasileirao.utils.temporada import partidas_esperadas

COLUNAS_OPCIONAIS_PARTIDAS = [
    "hora",
    "arena",
    "uf_mandante",
    "uf_visitante",
    "tecnico_mandante",
    "tecnico_visitante",
    "formacao_mandante",
    "formacao_visitante",
    "arrecadacao",
]
LIMITE_EXEMPLOS = 20


class QualidadeError(Exception):
    """Falha de qualidade. `problemas` lista cada checagem que falhou, com os valores."""

    def __init__(self, tabela: str, temporada: int, problemas: list[str]) -> None:
        self.problemas = problemas
        detalhe = "\n".join(f"- {p}" for p in problemas)
        super().__init__(f"silver/{tabela} temporada {temporada} reprovada:\n{detalhe}")


def times_sem_mapeamento(df: DataFrame, mapa: DataFrame, colunas: list[str]) -> list[str]:
    """Nomes que aparecem em `colunas` e não estão em `nome_fonte` do mapa, em ordem."""
    nomes = None
    for coluna in colunas:
        nomes_coluna = df.select(F.col(coluna).alias("nome")).where(F.col("nome").isNotNull())
        nomes = nomes_coluna if nomes is None else nomes.union(nomes_coluna)
    conhecidos = mapa.select(F.col("nome_fonte").alias("nome"))
    faltando = nomes.distinct().join(F.broadcast(conhecidos), on="nome", how="left_anti")
    return sorted(linha["nome"] for linha in faltando.collect())


def partida_ids_invalidos(df: DataFrame) -> tuple[int, list[int]]:
    """Nº de `partida_id` nulos e os ids duplicados (até `LIMITE_EXEMPLOS`)."""
    nulos = df.where(F.col("partida_id").isNull()).count()
    duplicados = (
        df.where(F.col("partida_id").isNotNull())
        .groupBy("partida_id")
        .count()
        .where(F.col("count") > 1)
        .orderBy("partida_id")
        .limit(LIMITE_EXEMPLOS)
        .collect()
    )
    return nulos, [linha["partida_id"] for linha in duplicados]


def placares_invalidos(df: DataFrame) -> list[int]:
    """Partidas com placar nulo, não numérico (vira nulo na limpeza) ou negativo."""
    invalido = F.lit(False)
    for coluna in ("placar_mandante", "placar_visitante"):
        invalido = invalido | F.col(coluna).isNull() | (F.col(coluna) < 0)
    linhas = df.where(invalido).orderBy("partida_id").limit(LIMITE_EXEMPLOS).collect()
    return [linha["partida_id"] for linha in linhas]


def checar_falhas_partidas(
    limpo: DataFrame, mapa: DataFrame, colunas_time: list[str], temporada: int
) -> None:
    """Levanta `QualidadeError` com todas as falhas encontradas. Recebe as partidas antes da
    normalização, para listar os nomes da fonte que faltam no mapa."""
    problemas = []
    faltando = times_sem_mapeamento(limpo, mapa, colunas_time)
    if faltando:
        problemas.append(f"times sem mapeamento em reference/times.csv: {', '.join(faltando)}")
    nulos, duplicados = partida_ids_invalidos(limpo)
    if nulos:
        problemas.append(f"{nulos} partida(s) com partida_id nulo")
    if duplicados:
        problemas.append(f"partida_id duplicado: {', '.join(map(str, duplicados))}")
    placares = placares_invalidos(limpo)
    if placares:
        problemas.append(
            f"placar nulo, não numérico ou negativo nas partidas: {', '.join(map(str, placares))}"
        )
    if problemas:
        raise QualidadeError("partidas", temporada, problemas)


def avisos_partidas(silver: DataFrame, temporada: int) -> list[str]:
    """Avisos que não interrompem a execução: contagem da temporada e nulos por coluna."""
    contagens = silver.agg(
        F.count(F.lit(1)).alias("_total"),
        *[F.count_if(F.col(c).isNull()).alias(c) for c in COLUNAS_OPCIONAIS_PARTIDAS],
    ).first()
    avisos = []
    esperado = partidas_esperadas(temporada)
    if contagens["_total"] != esperado:
        avisos.append(f"{contagens['_total']} partidas na temporada, esperado {esperado}")
    nulos = [f"{c}={contagens[c]}" for c in COLUNAS_OPCIONAIS_PARTIDAS if contagens[c]]
    if nulos:
        avisos.append(f"nulos em colunas opcionais: {', '.join(nulos)}")
    return avisos
