"""Tabelas de referência do gold, lidas do pacote para funcionar igual a partir do wheel."""

import csv
import io
from importlib import resources

from pyspark.sql import DataFrame, SparkSession

SCHEMA_AJUSTES = "temporada int, clube string, pontos int, motivo string, fonte string"


def carregar_ajustes(spark: SparkSession) -> DataFrame:
    """`reference/ajustes_pontos.csv`: punições do STJD, uma linha por clube afetado."""
    texto = (
        resources.files("brasileirao.reference").joinpath("ajustes_pontos.csv").read_text("utf-8")
    )
    linhas = [
        (int(r["temporada"]), r["clube"], int(r["pontos"]), r["motivo"], r["fonte"])
        for r in csv.DictReader(io.StringIO(texto))
    ]
    return spark.createDataFrame(linhas, SCHEMA_AJUSTES)
