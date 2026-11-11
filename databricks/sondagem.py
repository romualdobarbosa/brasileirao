"""Sondagem do serverless do Databricks: o que o pipeline usa e a documentação não garante.

Roda como `spark_python_task` com o wheel do projeto no environment (ver `submeter.py`).
Cada item vira uma linha de resultado (ok/falhou e o detalhe), impressa no log e gravada em
`<raiz>/sondagem/resultado.json`. Uma falha não interrompe os itens seguintes.
"""

import json
import sys
import traceback
from importlib import metadata

RAIZ = sys.argv[1] if len(sys.argv) > 1 else "/Volumes/workspace/default/brasileirao"
BASE = f"{RAIZ}/sondagem"
resultado: dict[str, dict] = {}


def item(nome):
    def decorador(funcao):
        try:
            resultado[nome] = {"ok": True, "detalhe": funcao()}
        except Exception as erro:  # noqa: BLE001 - a sondagem registra qualquer falha
            resultado[nome] = {
                "ok": False,
                "detalhe": f"{type(erro).__name__}: {erro}"[:2000],
                "trace": traceback.format_exc()[-3000:],
            }
        print(nome, json.dumps(resultado[nome], default=str)[:2500], flush=True)
        return funcao

    return decorador


@item("versoes")
def _():
    pacotes = [
        "brasileirao",
        "pyspark",
        "databricks-connect",
        "delta-spark",
        "kagglehub",
        "psycopg",
        "psycopg-binary",
        "protobuf",
        "certifi",
        "pyarrow",
        "pandas",
    ]
    versoes = {}
    for p in pacotes:
        try:
            versoes[p] = metadata.version(p)
        except metadata.PackageNotFoundError:
            versoes[p] = None
    versoes["python"] = sys.version
    return versoes


def _importar_em_subprocesso(modulo):
    # Num subprocesso: o import do psycopg derruba o Python com SIGABRT no serverless (visto
    # na primeira sondagem), e isso mataria a sondagem inteira.
    import subprocess

    feito = subprocess.run(
        [sys.executable, "-c", f"import {modulo}"], capture_output=True, text=True, timeout=300
    )
    if feito.returncode != 0:
        raise RuntimeError(f"código {feito.returncode}: {feito.stderr[-800:]}")
    return "importa"


@item("import_kagglehub")
def _():
    return _importar_em_subprocesso("kagglehub")


@item("import_psycopg")
def _():
    return _importar_em_subprocesso("psycopg")


@item("import_brasileirao_pipeline")
def _():
    import brasileirao.pipeline  # noqa: F401

    return {
        "psycopg_carregado": "psycopg" in sys.modules,
        "kagglehub_carregado": "kagglehub" in sys.modules,
    }


from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

spark = SparkSession.builder.getOrCreate()


@item("sessao")
def _():
    return {
        "classe": type(spark).__module__ + "." + type(spark).__name__,
        "versao": spark.version,
        "timezone": spark.conf.get("spark.sql.session.timeZone"),
    }


@item("conf_shuffle_partitions")
def _():
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    return spark.conf.get("spark.sql.shuffle.partitions")


@item("conf_snapshot_partitions")
def _():
    spark.conf.set("spark.databricks.delta.snapshotPartitions", "2")
    return spark.conf.get("spark.databricks.delta.snapshotPartitions")


@item("get_spark_databricks")
def _():
    import os

    os.environ["BRASILEIRAO_SPARK_ENV"] = "databricks"
    from brasileirao.utils.spark import get_spark

    return type(get_spark()).__name__


@item("local_checkpoint")
def _():
    df = spark.range(1000).withColumn("dobro", F.col("id") * 2).localCheckpoint()
    return {"count": df.count(), "soma": df.agg(F.sum("dobro")).first()[0]}


@item("cache")
def _():
    df = spark.range(1000).withColumn("dobro", F.col("id") * 2).cache()
    return {
        "count": df.count(),
        "soma": df.agg(F.sum("dobro")).first()[0],
        "storage_level": str(df.storageLevel),
    }


@item("metadata_file_path")
def _():
    df = (
        spark.read.option("header", True)
        .csv(f"{RAIZ}/raw/campeonato-brasileiro-gols.csv")
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    return df.select("_source_file").first()[0]


@item("delta_tables_import")
def _():
    from delta.tables import DeltaTable

    return DeltaTable.__module__


def _escrever_alvo(caminho):
    (
        spark.createDataFrame(
            [(1, "a", 2020), (2, "b", 2020), (3, "c", 2021)], "k long, v string, temporada int"
        )
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("temporada")
        .save(caminho)
    )


@item("delta_forpath_merge")
def _():
    from delta.tables import DeltaTable

    caminho = f"{BASE}/merge"
    _escrever_alvo(caminho)
    alvo = DeltaTable.forPath(spark, caminho)
    novo = spark.createDataFrame(
        [(2, "B", 2020), (4, "d", 2020)], "k long, v string, temporada int"
    )
    (
        alvo.alias("s")
        .merge(novo.alias("n"), "s.k = n.k")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    alvo.delete(F.col("k") == 1)
    linhas = sorted(tuple(r) for r in alvo.toDF().collect())
    return {
        "isDeltaTable": DeltaTable.isDeltaTable(spark, caminho),
        "isDeltaTable_raw": DeltaTable.isDeltaTable(spark, f"{RAIZ}/raw"),
        "linhas": linhas,
        "versoes": alvo.history().count(),
        "detail_properties": dict(alvo.detail().first()["properties"]),
    }


def _padrao_merge_silver(modo):
    """O padrão de `gravar_merge`: fonte do MERGE = linhas novas/alteradas contra o alvo."""
    import os

    from delta.tables import DeltaTable

    os.environ["BRASILEIRAO_SPARK_ENV"] = "databricks"
    os.environ["BRASILEIRAO_MATERIALIZAR"] = modo
    from brasileirao.utils.spark import materializar

    caminho = f"{BASE}/merge_{modo}"
    _escrever_alvo(caminho)
    alvo = DeltaTable.forPath(spark, caminho)
    atual = alvo.toDF()
    df = spark.createDataFrame(
        [(1, "a", 2020), (2, "B", 2020), (4, "d", 2020)], "k long, v string, temporada int"
    )
    igual = [F.col(f"n.{c}").eqNullSafe(F.col(f"s.{c}")) for c in df.columns]
    alterados = materializar(df.alias("n").join(atual.alias("s"), on=igual, how="left_anti"))
    antes = sorted(tuple(r) for r in alterados.collect())
    (
        alvo.alias("s")
        .merge(alterados.alias("n"), "s.k = n.k")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    depois = sorted(tuple(r) for r in alterados.collect())
    return {
        "alterados_antes": antes,
        "alterados_depois_do_merge": depois,
        "estavel": antes == depois,
        "alvo": sorted(tuple(r) for r in alvo.toDF().collect()),
    }


@item("merge_silver_local_checkpoint")
def _():
    return _padrao_merge_silver("local_checkpoint")


@item("merge_silver_cache")
def _():
    return _padrao_merge_silver("cache")


@item("replace_where")
def _():
    caminho = f"{BASE}/replace"
    _escrever_alvo(caminho)
    (
        spark.createDataFrame([(9, "z", 2020)], "k long, v string, temporada int")
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("temporada")
        .option("replaceWhere", "temporada = 2020")
        .save(caminho)
    )
    return sorted(tuple(r) for r in spark.read.format("delta").load(caminho).collect())


@item("add_constraint_por_caminho")
def _():
    caminho = f"{BASE}/merge"
    spark.sql(f"ALTER TABLE delta.`{caminho}` ADD CONSTRAINT k_positivo CHECK (k > 0)")
    from delta.tables import DeltaTable

    props = DeltaTable.forPath(spark, caminho).detail().first()["properties"]
    return {k: v for k, v in props.items() if k.startswith("delta.constraints.")}


@item("add_constraint_violada")
def _():
    from pyspark.errors import AnalysisException

    caminho = f"{BASE}/merge"
    try:
        spark.sql(f"ALTER TABLE delta.`{caminho}` ADD CONSTRAINT k_grande CHECK (k > 100)")
    except AnalysisException as erro:
        return {
            "classe": type(erro).__module__ + "." + type(erro).__name__,
            "condition": erro.getCondition(),
        }
    raise AssertionError("a constraint violada entrou")


@item("constraint_barra_escrita")
def _():
    caminho = f"{BASE}/merge"
    try:
        (
            spark.createDataFrame([(-1, "x", 2020)], "k long, v string, temporada int")
            .write.format("delta")
            .mode("append")
            .save(caminho)
        )
    except Exception as erro:  # noqa: BLE001
        return {"classe": type(erro).__name__, "mensagem": str(erro)[:300]}
    raise AssertionError("a linha que viola a constraint foi gravada")


@item("escrita_temporaria")
def _():
    caminho = f"{BASE}/temporario"
    df = spark.range(10).withColumn("dobro", F.col("id") * 2)
    df.write.format("delta").mode("overwrite").save(caminho)
    lido = spark.read.format("delta").load(caminho)
    return lido.count()


saida = json.dumps(resultado, indent=1, default=str)
try:
    from databricks.sdk.runtime import dbutils

    dbutils.fs.put(f"{BASE}/resultado.json", saida, True)
except Exception as erro:  # noqa: BLE001
    print("não gravou resultado.json:", erro)
    with open(f"{BASE}/resultado.json", "w") as arquivo:
        arquivo.write(saida)
print("FIM", sum(r["ok"] for r in resultado.values()), "de", len(resultado), "ok")
