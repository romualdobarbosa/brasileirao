"""Foto das camadas: linhas e sha256 das linhas ordenadas, por tabela.

`foto.py <data_root> <saida.json> [camada ...]`. O mesmo script roda no local
(`uv run python databricks/foto.py ./data foto.json`) e como task do job no Databricks,
para comparar as duas execuções tabela a tabela.
"""

import hashlib
import json
import os
import sys

from pyspark.sql import SparkSession

CAMADAS_PADRAO = ("silver", "gold")


def sessao(raiz: str) -> SparkSession:
    if not raiz.startswith("/Volumes/"):
        from brasileirao.utils.spark import get_spark

        return get_spark(conf={"spark.ui.enabled": "false"})
    return SparkSession.builder.getOrCreate()


def tabelas(raiz: str) -> list[str]:
    if raiz.startswith("/Volumes/"):
        from databricks.sdk.runtime import dbutils

        return sorted(i.name.rstrip("/") for i in dbutils.fs.ls(raiz))
    return sorted(os.listdir(raiz))


def foto(spark: SparkSession, raiz: str, camadas: list[str]) -> dict:
    resultado = {}
    for camada in camadas:
        for tabela in tabelas(f"{raiz}/{camada}"):
            caminho = f"{raiz}/{camada}/{tabela}"
            df = spark.read.format("delta").load(caminho)
            linhas = sorted(
                json.dumps(r.asDict(recursive=True), default=str, sort_keys=True)
                for r in df.collect()
            )
            resultado[f"{camada}/{tabela}"] = {
                "linhas": len(linhas),
                "sha256": hashlib.sha256("\n".join(linhas).encode()).hexdigest(),
            }
    return resultado


if __name__ == "__main__":
    raiz, saida, *camadas = sys.argv[1:]
    resultado = foto(sessao(raiz), raiz.rstrip("/"), camadas or list(CAMADAS_PADRAO))
    texto = json.dumps(resultado, indent=1, sort_keys=True)
    print(texto)
    with open(saida, "w") as arquivo:
        arquivo.write(texto)
    print(len(resultado), "tabelas")
