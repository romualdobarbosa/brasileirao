"""Gold (Delta) → um Parquet por tabela, para o app publicado ler sem Spark."""

import logging
import os
import tempfile
from pathlib import Path

from delta.tables import DeltaTable
from py4j.protocol import Py4JJavaError
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from brasileirao import config
from brasileirao.aggregate import gold

log = logging.getLogger(__name__)

DESTINO_PADRAO = "app/dados"


class GoldIncompletoError(FileNotFoundError):
    pass


def exportar(spark: SparkSession, destino: str = DESTINO_PADRAO) -> dict[str, int]:
    """Grava `<destino>/<tabela>.parquet` para cada tabela do registro. Devolve as linhas.

    Uma partição só, ordenada por todas as colunas: arquivo único e sempre na mesma ordem. Os
    arquivos só trocam depois de todas as tabelas gravadas, então um erro no meio não deixa o
    app com meia exportação. Arquivo com o mesmo conteúdo fica como está: o Spark escreve os
    encodings no rodapé em ordem que muda a cada JVM, e o git acusaria mudança sem haver uma.
    """
    caminhos = {nome: config.caminho_tabela("gold", nome) for nome in gold.TABELAS}
    for nome, caminho in caminhos.items():
        if not DeltaTable.isDeltaTable(spark, caminho):
            raise GoldIncompletoError(f"gold/{nome} não existe em {caminho}: rode `make gold`")

    pasta = Path(destino).resolve()
    pasta.mkdir(parents=True, exist_ok=True)
    linhas = {}
    # Dentro do destino, para o `os.replace` não cruzar sistema de arquivos.
    with tempfile.TemporaryDirectory(dir=pasta, prefix=".exportando-") as temporario:
        prontos = {}
        for nome, caminho in caminhos.items():
            df = spark.read.format("delta").load(caminho)
            saida = Path(temporario) / nome
            df.repartition(1).sortWithinPartitions(*df.columns).write.parquet(saida.as_uri())
            prontos[nome] = _unico_parquet(saida)
            linhas[nome] = df.count()
        for nome, arquivo in prontos.items():
            final = pasta / f"{nome}.parquet"
            if _mesmo_conteudo(spark, arquivo, final):
                log.info("%s/%s.parquet: sem mudança", destino, nome)
            else:
                os.replace(arquivo, final)
                log.info("%s/%s.parquet: %d linhas", destino, nome, linhas[nome])
    return linhas


def _unico_parquet(pasta: Path) -> Path:
    # Tabela vazia também gera um arquivo: o Spark grava a partição com o schema.
    arquivos = list(pasta.glob("part-*.parquet"))
    if len(arquivos) != 1:
        raise RuntimeError(f"Esperava um Parquet em {pasta}, achei {len(arquivos)}")
    return arquivos[0]


def _mesmo_conteudo(spark: SparkSession, novo: Path, atual: Path) -> bool:
    """Mesmo schema e mesmas linhas na mesma ordem. Arquivo ausente ou ilegível não é igual."""
    if not atual.exists():
        return False
    try:
        df_atual = spark.read.parquet(atual.as_uri())
        df_novo = spark.read.parquet(novo.as_uri())
        return df_atual.schema == df_novo.schema and df_atual.collect() == df_novo.collect()
    except (PySparkException, Py4JJavaError):
        return False
