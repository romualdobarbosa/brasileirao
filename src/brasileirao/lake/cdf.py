"""Change Data Feed: liga na tabela e lê o que mudou entre duas versões.

Ligar não mexe no dado. O Delta passa a gravar, a cada `MERGE`, `UPDATE` ou `DELETE`, as linhas
afetadas em `_change_data/`, e a leitura com `readChangeFeed` devolve essas linhas com
`_change_type` (`insert`, `update_preimage`, `update_postimage`, `delete`) e `_commit_version`.
"""

import logging
import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from brasileirao.lake import TabelaInvalidaError, caminho_de

log = logging.getLogger(__name__)

PROPRIEDADE = "delta.enableChangeDataFeed"
# Tabelas com o feed ligado. Só a `silver/partidas` recebe `MERGE`; as outras trocam a partição
# inteira, e o feed delas seria a temporada toda apagada e inserida de novo.
TABELAS_COM_CDF = ("silver/partidas",)


def _referencia_sql(caminho: str) -> str:
    """Como em `constraints`: o SQL só reconhece caminho absoluto. Repetido aqui porque
    `constraints` importa o gold, e a silver, que importa este módulo, vem antes dele."""
    absoluto = caminho if "://" in caminho else os.path.abspath(caminho)
    return f"delta.`{absoluto}`"


def ligado(spark: SparkSession, tabela: str) -> bool:
    caminho = caminho_de(tabela)
    propriedades = DeltaTable.forPath(spark, caminho).detail().first()["properties"]
    return propriedades.get(PROPRIEDADE, "false").lower() == "true"


def ligar(spark: SparkSession, tabela: str) -> bool:
    """Liga o feed se ainda não estiver ligado. Devolve se mudou algo.

    Já ligado, não toca na tabela: o `SET TBLPROPERTIES` grava uma versão mesmo sem mudança, e
    rodar a mesma rodada de novo não pode criar versão.
    """
    caminho = caminho_de(tabela)
    if not DeltaTable.isDeltaTable(spark, caminho):
        raise TabelaInvalidaError(f"{tabela} não é uma tabela Delta em {caminho}")
    if ligado(spark, tabela):
        return False
    spark.sql(f"ALTER TABLE {_referencia_sql(caminho)} SET TBLPROPERTIES ({PROPRIEDADE} = true)")
    log.info("%s: Change Data Feed ligado", tabela)
    return True


def mudancas(spark: SparkSession, tabela: str, inicio: int, fim: int) -> DataFrame:
    """Linhas do feed das versões `inicio` a `fim`, inclusive."""
    return (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", inicio)
        .option("endingVersion", fim)
        .load(caminho_de(tabela))
    )
