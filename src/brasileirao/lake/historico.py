"""Time travel: lista as versões de uma tabela Delta e compara duas delas."""

from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from brasileirao.lake import TabelaInvalidaError, caminho_de

METRICAS = {
    "numOutputRows",
    "numFiles",
    "numAddedFiles",
    "numRemovedFiles",
    "numTargetRowsInserted",
    "numTargetRowsUpdated",
    "numTargetRowsDeleted",
    "numFilesToDelete",
    "numDeletedFiles",
}


VERSAO_FORA_DE_ALCANCE = {
    "DELTA_VERSION_NOT_FOUND",
    "DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION",
}


class VersaoIndisponivelError(LookupError):
    """A versão não existe no histórico ou já passou da retenção da tabela."""


@dataclass
class ComparacaoVersoes:
    """O que mudou da `versao_a` para a `versao_b`, comparando só as colunas em comum.

    Sem chave, `alteradas` fica `None`, e uma linha alterada aparece nas duas pontas: a antiga
    em `removidas` e a nova em `inseridas`. Com chave, `inseridas` e `removidas` só têm chaves
    que entraram ou saíram, e `alteradas` tem a linha nova de cada chave que mudou.
    """

    versao_a: int
    versao_b: int
    linhas_a: int
    linhas_b: int
    colunas_adicionadas: list[str]
    colunas_removidas: list[str]
    inseridas: DataFrame
    removidas: DataFrame
    alteradas: DataFrame | None

    def resumo(self) -> str:
        partes = [
            f"versão {self.versao_a}: {self.linhas_a} linhas",
            f"versão {self.versao_b}: {self.linhas_b} linhas",
            f"inseridas: {self.inseridas.count()}",
            f"removidas: {self.removidas.count()}",
        ]
        if self.alteradas is not None:
            partes.append(f"alteradas: {self.alteradas.count()}")
        if self.colunas_adicionadas:
            partes.append(f"colunas adicionadas: {', '.join(self.colunas_adicionadas)}")
        if self.colunas_removidas:
            partes.append(f"colunas removidas: {', '.join(self.colunas_removidas)}")
        return "\n".join(partes)


def _exigir_tabela(spark: SparkSession, tabela: str) -> str:
    caminho = caminho_de(tabela)
    if not DeltaTable.isDeltaTable(spark, caminho):
        raise TabelaInvalidaError(f"{tabela} não é uma tabela Delta em {caminho}")
    return caminho


def historico(spark: SparkSession, tabela: str) -> DataFrame:
    """Uma linha por versão, da mais nova para a mais antiga."""
    caminho = _exigir_tabela(spark, tabela)
    return (
        DeltaTable.forPath(spark, caminho)
        .history()
        .select(
            "version",
            F.date_format("timestamp", "yyyy-MM-dd HH:mm:ss").alias("timestamp"),
            "operation",
            "operationParameters",
            "operationMetrics",
        )
    )


def ler_versao(spark: SparkSession, tabela: str, versao: int) -> DataFrame:
    """A tabela como estava na `versao`.

    Versão fora do histórico ou mais velha que a retenção (7 dias) levanta aqui. Dentro da
    retenção, se os arquivos sumiram, só falha quando alguém lê as linhas: o log ainda existe,
    e o `count()` responde pelas estatísticas dele sem abrir arquivo.
    """
    caminho = _exigir_tabela(spark, tabela)
    try:
        return spark.read.format("delta").option("versionAsOf", versao).load(caminho)
    except AnalysisException as erro:
        if erro.getCondition() not in VERSAO_FORA_DE_ALCANCE:
            raise
        raise VersaoIndisponivelError(f"{tabela} não tem a versão {versao} disponível") from erro


def comparar_versoes(
    spark: SparkSession,
    tabela: str,
    versao_a: int,
    versao_b: int,
    chave: list[str] | None = None,
) -> ComparacaoVersoes:
    """Diferença entre duas versões, linha a linha, respeitando linhas repetidas."""
    antes = ler_versao(spark, tabela, versao_a)
    depois = ler_versao(spark, tabela, versao_b)
    comuns = [c for c in depois.columns if c in antes.columns]
    so_antes = antes.select(*comuns).exceptAll(depois.select(*comuns))
    so_depois = depois.select(*comuns).exceptAll(antes.select(*comuns))
    if chave is None:
        inseridas, removidas, alteradas = so_depois, so_antes, None
    else:
        inseridas = so_depois.join(antes.select(*chave), on=chave, how="left_anti")
        removidas = so_antes.join(depois.select(*chave), on=chave, how="left_anti")
        alteradas = so_depois.join(antes.select(*chave), on=chave, how="left_semi")
    return ComparacaoVersoes(
        versao_a=versao_a,
        versao_b=versao_b,
        linhas_a=antes.count(),
        linhas_b=depois.count(),
        colunas_adicionadas=[c for c in depois.columns if c not in antes.columns],
        colunas_removidas=[c for c in antes.columns if c not in depois.columns],
        inseridas=inseridas,
        removidas=removidas,
        alteradas=alteradas,
    )


def imprimir_historico(spark: SparkSession, tabela: str) -> None:
    linhas = historico(spark, tabela).collect()
    print(f"{tabela}: {len(linhas)} versão(ões) no histórico")
    print(f"{'versão':>6}  {'quando':<19}  {'operação':<16}  métricas")
    for linha in linhas:
        metricas = linha["operationMetrics"] or {}
        texto = ", ".join(f"{k}={v}" for k, v in sorted(metricas.items()) if k in METRICAS)
        print(f"{linha['version']:>6}  {linha['timestamp']:<19}  {linha['operation']:<16}  {texto}")
