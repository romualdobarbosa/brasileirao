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


def processar_silver(spark: SparkSession, temporada: int) -> list[CargaSilver]:
    """Prepara e checa todas as tabelas da temporada antes de gravar qualquer uma."""
    mapa = carregar_mapa_times(spark)
    cargas = [preparar_partidas(spark, temporada, mapa)]
    for carga in cargas:
        if carga.tabela == "partidas":
            gravar_merge(spark, carga.df, "partidas", "partida_id", temporada)
        for aviso in carga.avisos:
            log.warning("silver/%s temporada %d: %s", carga.tabela, temporada, aviso)
    return cargas
