"""Gold (Delta) → Postgres: grava em gold_carga via JDBC, troca numa transação e confere."""

import logging
import time

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from brasileirao import config
from brasileirao.serving import postgres
from brasileirao.serving.tabelas import TABELAS

log = logging.getLogger(__name__)


class GoldAusenteError(FileNotFoundError):
    pass


class CargaError(RuntimeError):
    pass


def ler_gold(spark: SparkSession) -> dict[str, DataFrame]:
    """As 8 tabelas gold, com as colunas na ordem de `TABELAS`. Falha antes de gravar
    qualquer coisa se faltar tabela ou se o schema tiver mudado."""
    tabelas = {}
    for tabela in TABELAS:
        caminho = config.caminho_tabela("gold", tabela.nome)
        if not DeltaTable.isDeltaTable(spark, caminho):
            raise GoldAusenteError(
                f"gold/{tabela.nome} não existe em {caminho}: rode `make gold` antes"
            )
        df = spark.read.format("delta").load(caminho)
        if tuple(df.columns) != tabela.colunas:
            raise CargaError(
                f"gold/{tabela.nome} mudou de schema: tem {df.columns}, o Postgres espera "
                f"{list(tabela.colunas)} (serving/tabelas.py e initdb/02-gold.sql)"
            )
        tabelas[tabela.nome] = df
    return tabelas


def carregar(spark: SparkSession) -> dict[str, int]:
    """Lê o gold, grava em gold_carga, troca gold numa transação e confere as contagens.

    Devolve as linhas de cada tabela em `gold`.
    """
    inicio = time.monotonic()
    gold = ler_gold(spark)
    url, propriedades = config.jdbc_postgres()
    # Conecta antes de gravar: Postgres fora do ar falha aqui, com a mensagem do psycopg.
    with postgres.conectar() as conexao:
        esperadas = {}
        for nome, df in gold.items():
            esperadas[nome] = df.count()
            df.write.jdbc(url, f"gold_carga.{nome}", mode="overwrite", properties=propriedades)
        postgres.trocar_gold(conexao)
        contagens = postgres.contar(conexao, "gold")
    divergentes = [
        f"{nome}: Delta {esperadas[nome]}, Postgres {contagens[nome]}"
        for nome in esperadas
        if contagens[nome] != esperadas[nome]
    ]
    if divergentes:
        raise CargaError("contagens diferentes depois da troca: " + "; ".join(divergentes))
    for nome, linhas in contagens.items():
        log.info("gold.%s: %d linhas", nome, linhas)
    log.info("carga: %d tabela(s) em %.1fs", len(contagens), time.monotonic() - inicio)
    return contagens
