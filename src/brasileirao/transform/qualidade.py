"""Checagens da silver. Rodam antes da escrita: uma falha não deixa a tabela pela metade."""

from pyspark.sql import DataFrame, Row
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
COLUNAS_OPCIONAIS_FILHAS = {
    "gols": ["atleta", "minuto"],
    "cartoes": ["atleta", "num_camisa", "posicao", "minuto"],
    "estatisticas": ["posse_de_bola", "precisao_passes"],
}
CARTOES_VALIDOS = ("Amarelo", "Vermelho")
LIMITE_EXEMPLOS = 20
TABELA_INTEIRA = "*"


def buracos_conhecidos(tabela: str, temporada: int) -> set[str]:
    """Colunas sem dado na fonte nesta temporada (tabela "Cobertura da fonte" da spec).

    Nulo num buraco conhecido não gera aviso. `TABELA_INTEIRA` quer dizer que a fonte não tem
    nenhuma linha da tabela na temporada.
    """
    buracos = set()
    if tabela == "partidas":
        if temporada < 2014:
            buracos |= {"tecnico_mandante", "tecnico_visitante"}
        if temporada <= 2014:
            buracos |= {"formacao_mandante", "formacao_visitante"}
        if temporada < 2025:
            buracos.add("arrecadacao")
        return buracos
    sem_dado = {
        "gols": temporada < 2014,
        "cartoes": temporada < 2014 or temporada == 2025,
        "estatisticas": temporada < 2014 or temporada == 2024,
    }
    if sem_dado[tabela]:
        return {TABELA_INTEIRA}
    if tabela == "cartoes":
        buracos.add("posicao")
        if temporada == 2014:
            buracos.add("num_camisa")
    if tabela == "estatisticas" and temporada <= 2017:
        buracos.add("precisao_passes")
    return buracos


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


def _total_e_nulos(silver: DataFrame, colunas: list[str]) -> Row:
    return silver.agg(
        F.count(F.lit(1)).alias("_total"),
        *[F.count_if(F.col(c).isNull()).alias(c) for c in colunas],
    ).first()


def _aviso_de_nulos(contagens: Row, colunas: list[str], buracos: set[str]) -> list[str]:
    nulos = [f"{c}={contagens[c]}" for c in colunas if contagens[c] and c not in buracos]
    return [f"nulos em colunas opcionais: {', '.join(nulos)}"] if nulos else []


def avisos_partidas(silver: DataFrame, temporada: int) -> list[str]:
    """Avisos que não interrompem a execução: contagem da temporada e nulos por coluna."""
    contagens = _total_e_nulos(silver, COLUNAS_OPCIONAIS_PARTIDAS)
    avisos = []
    esperado = partidas_esperadas(temporada)
    if contagens["_total"] != esperado:
        avisos.append(f"{contagens['_total']} partidas na temporada, esperado {esperado}")
    buracos = buracos_conhecidos("partidas", temporada)
    return avisos + _aviso_de_nulos(contagens, COLUNAS_OPCIONAIS_PARTIDAS, buracos)


def avisos_filha(silver: DataFrame, tabela: str, temporada: int) -> list[str]:
    """Temporada sem nenhuma linha fora dos buracos conhecidos e nulos por coluna."""
    buracos = buracos_conhecidos(tabela, temporada)
    if TABELA_INTEIRA in buracos:
        return []
    colunas = COLUNAS_OPCIONAIS_FILHAS[tabela]
    contagens = _total_e_nulos(silver, colunas)
    if contagens["_total"] == 0:
        return ["nenhuma linha na temporada"]
    return _aviso_de_nulos(contagens, colunas, buracos)


def cartoes_invalidos(df: DataFrame) -> list[str]:
    """Valores de `cartao` fora de Amarelo/Vermelho (nulo incluído), em ordem."""
    invalidos = df.where(~F.coalesce(F.col("cartao").isin(*CARTOES_VALIDOS), F.lit(False)))
    linhas = invalidos.select("cartao").distinct().collect()
    return sorted(str(linha["cartao"]) for linha in linhas)


def chaves_duplicadas(df: DataFrame, chave: list[str]) -> list[str]:
    """Combinações de `chave` que aparecem mais de uma vez (até `LIMITE_EXEMPLOS`)."""
    linhas = (
        df.groupBy(*chave)
        .count()
        .where(F.col("count") > 1)
        .orderBy(*chave)
        .limit(LIMITE_EXEMPLOS)
        .collect()
    )
    return ["(" + ", ".join(str(linha[c]) for c in chave) + ")" for linha in linhas]


def checar_falhas_filha(limpo: DataFrame, mapa: DataFrame, tabela: str, temporada: int) -> None:
    """Falhas de uma filha antes da normalização: clube sem mapa e, em cartões, tipo de
    cartão desconhecido."""
    problemas = []
    faltando = times_sem_mapeamento(limpo, mapa, ["clube"])
    if faltando:
        problemas.append(f"times sem mapeamento em reference/times.csv: {', '.join(faltando)}")
    if tabela == "cartoes":
        invalidos = cartoes_invalidos(limpo)
        if invalidos:
            problemas.append(f"cartao fora de {'/'.join(CARTOES_VALIDOS)}: {', '.join(invalidos)}")
    if problemas:
        raise QualidadeError(tabela, temporada, problemas)


def checar_unicidade_estatisticas(silver: DataFrame, temporada: int) -> None:
    """Uma linha por (`partida_id`, `clube`), já com o clube canônico."""
    duplicadas = chaves_duplicadas(silver, ["partida_id", "clube"])
    if duplicadas:
        raise QualidadeError(
            "estatisticas",
            temporada,
            [f"(partida_id, clube) duplicado: {', '.join(duplicadas)}"],
        )


def partida_ids_orfaos(filha: DataFrame, partidas: DataFrame) -> tuple[int, list[str]]:
    """Linhas da filha cujo `partida_id` não está em `partidas`: total e ids (até
    `LIMITE_EXEMPLOS`). As duas entram com a coluna `partida_id`."""
    # Órfã é exceção: agrupar por id e trazer tudo num job só sai mais barato que contar e
    # listar separado.
    orfas = (
        filha.join(partidas.select("partida_id").distinct(), on="partida_id", how="left_anti")
        .groupBy("partida_id")
        .count()
        .collect()
    )
    ids = sorted(str(linha["partida_id"]) for linha in orfas)
    return sum(linha["count"] for linha in orfas), ids[:LIMITE_EXEMPLOS]
