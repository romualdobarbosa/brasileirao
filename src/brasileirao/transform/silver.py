"""Bronze → silver: lê a bronze da temporada, transforma, checa e grava."""

import csv
import io
import logging
from dataclasses import dataclass
from importlib import resources

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from brasileirao import config
from brasileirao.transform import limpeza, qualidade

log = logging.getLogger(__name__)

SCHEMA_MAPA_TIMES = "nome_fonte string, nome_canonico string, uf string"
LIMPEZA_FILHAS = {
    "gols": (limpeza.limpar_gols, limpeza.COLUNAS_GOLS),
    "cartoes": (limpeza.limpar_cartoes, limpeza.COLUNAS_CARTOES),
    "estatisticas": (limpeza.limpar_estatisticas, limpeza.COLUNAS_ESTATISTICAS),
}
TABELAS = ("partidas", *LIMPEZA_FILHAS)


@dataclass
class CargaSilver:
    """Uma tabela silver da temporada já transformada e aprovada nas checagens de falha."""

    tabela: str
    df: DataFrame
    avisos: list[str]


def carregar_mapa_times(spark: SparkSession) -> DataFrame:
    """`reference/times.csv` lido do pacote, para funcionar igual a partir do wheel."""
    texto = resources.files("brasileirao.reference").joinpath("times.csv").read_text("utf-8")
    linhas = [tuple(linha.values()) for linha in csv.DictReader(io.StringIO(texto))]
    return spark.createDataFrame(linhas, SCHEMA_MAPA_TIMES)


def ler_bronze(spark: SparkSession, entidade: str, temporada: int) -> DataFrame:
    caminho = config.caminho_tabela("bronze", entidade)
    return spark.read.format("delta").load(caminho).where(F.col("temporada") == temporada)


def preparar_partidas(spark: SparkSession, temporada: int, mapa: DataFrame) -> CargaSilver:
    # Uma temporada cabe folgada em memória; materializar evita recalcular a partir da
    # bronze em cada checagem.
    limpo = limpeza.deduplicar_partidas(
        limpeza.limpar_partidas(ler_bronze(spark, "partidas", temporada))
    ).localCheckpoint()
    qualidade.checar_falhas_partidas(limpo, mapa, limpeza.COLUNAS_TIME_PARTIDAS, temporada)
    df = limpeza.para_silver_partidas(limpo, mapa).localCheckpoint()
    return CargaSilver("partidas", df, qualidade.avisos_partidas(df, temporada))


def preparar_filha(
    spark: SparkSession, tabela: str, temporada: int, mapa: DataFrame
) -> CargaSilver:
    limpar, colunas = LIMPEZA_FILHAS[tabela]
    limpo = limpar(ler_bronze(spark, tabela, temporada))
    if tabela == "estatisticas":
        limpo = limpeza.descartar_estatisticas_sem_dado(limpo)
    limpo = limpo.localCheckpoint()
    qualidade.checar_falhas_filha(limpo, mapa, tabela, temporada)
    df = limpeza.para_silver_filha(limpo, mapa, colunas).localCheckpoint()
    if tabela == "estatisticas":
        qualidade.checar_unicidade_estatisticas(df, temporada)
    return CargaSilver(tabela, df, qualidade.avisos_filha(df, tabela, temporada))


def gravar_particao(df: DataFrame, tabela: str, temporada: int) -> None:
    """Substitui a partição da temporada inteira (overwrite + replaceWhere).

    As filhas não têm chave natural confiável; trocar a temporada inteira é idempotente por
    construção. Temporada vazia apaga a partição.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("temporada")
        .option("replaceWhere", f"temporada = {temporada}")
        .save(config.caminho_tabela("silver", tabela))
    )


def gravar_merge(
    spark: SparkSession, df: DataFrame, tabela: str, chave: str, temporada: int
) -> None:
    """MERGE pela chave natural. Rodar de novo com o mesmo dado não cria versão nova.

    A fonte do MERGE leva só as linhas novas ou alteradas: o Delta reescreve todo arquivo
    com linha casada, mesmo quando a condição do update é falsa. A linha da temporada que
    saiu da fonte é apagada, para a silver não guardar o que a bronze já não tem.
    """
    caminho = config.caminho_tabela("silver", tabela)
    if not DeltaTable.isDeltaTable(spark, caminho):
        df.write.format("delta").partitionBy("temporada").save(caminho)
        return
    alvo = DeltaTable.forPath(spark, caminho)
    atual = alvo.toDF()
    igual = [F.col(f"n.{c}").eqNullSafe(F.col(f"s.{c}")) for c in df.columns]
    # Materializado antes do MERGE porque depende da própria tabela de destino.
    alterados = df.alias("n").join(atual.alias("s"), on=igual, how="left_anti").localCheckpoint()
    da_temporada = F.col("temporada") == temporada
    sobrando = [
        linha[chave]
        for linha in atual.where(da_temporada)
        .join(df.select(chave), on=chave, how="left_anti")
        .select(chave)
        .collect()
    ]
    if not alterados.isEmpty():
        (
            alvo.alias("s")
            .merge(alterados.alias("n"), f"s.{chave} = n.{chave}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    if sobrando:
        alvo.delete(da_temporada & F.col(chave).isin(sobrando))


def processar_silver(
    spark: SparkSession, temporada: int, tabelas: tuple[str, ...] = TABELAS
) -> list[CargaSilver]:
    """Prepara e checa todas as tabelas da temporada antes de gravar qualquer uma."""
    mapa = carregar_mapa_times(spark)
    cargas = [
        preparar_partidas(spark, temporada, mapa)
        if tabela == "partidas"
        else preparar_filha(spark, tabela, temporada, mapa)
        for tabela in tabelas
    ]
    for carga in cargas:
        if carga.tabela == "partidas":
            gravar_merge(spark, carga.df, "partidas", "partida_id", temporada)
        else:
            gravar_particao(carga.df, carga.tabela, temporada)
        log.info("silver/%s temporada %d: %d linhas", carga.tabela, temporada, carga.df.count())
        for aviso in carga.avisos:
            log.warning("silver/%s temporada %d: %s", carga.tabela, temporada, aviso)
    return cargas
