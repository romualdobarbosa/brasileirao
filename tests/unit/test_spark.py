import re
import subprocess
import sys
import tomllib
from pathlib import Path

import delta
import pytest
from pyspark.sql import SparkSession

from brasileirao.utils.spark import get_spark, materializar


def test_ambiente_desconhecido(monkeypatch):
    monkeypatch.setenv("BRASILEIRAO_SPARK_ENV", "marte")
    with pytest.raises(ValueError, match="marte"):
        get_spark()


@pytest.mark.parametrize("ambiente", ["emr"])
def test_ambiente_futuro_nao_implementado(monkeypatch, ambiente):
    monkeypatch.setenv("BRASILEIRAO_SPARK_ENV", ambiente)
    with pytest.raises(NotImplementedError, match=ambiente):
        get_spark()


def test_pacotes_extras_vao_para_o_delta_pip(monkeypatch):
    recebidos = {}

    class BuilderFalso:
        def getOrCreate(self):
            return "sessao"

    def configurar(builder, extra_packages=None):
        recebidos["pacotes"] = extra_packages
        return BuilderFalso()

    monkeypatch.setenv("BRASILEIRAO_SPARK_ENV", "local")
    monkeypatch.setattr(delta, "configure_spark_with_delta_pip", configurar)

    assert get_spark(pacotes=["org.postgresql:postgresql:42.7.14"]) == "sessao"
    assert recebidos["pacotes"] == ["org.postgresql:postgresql:42.7.14"]


class ConfFalsa(dict):
    def set(self, chave, valor):
        self[chave] = valor


class SessaoFalsa:
    def __init__(self):
        self.conf = ConfFalsa()


@pytest.fixture
def databricks(monkeypatch):
    """Ambiente databricks sem Databricks: a sessão ativa é um dublê, e o caminho local falha."""
    monkeypatch.setenv("BRASILEIRAO_SPARK_ENV", "databricks")

    def proibido(*args, **kwargs):
        raise AssertionError("o ambiente databricks não configura sessão pelo delta pip")

    monkeypatch.setattr(delta, "configure_spark_with_delta_pip", proibido)
    sessao = SessaoFalsa()
    monkeypatch.setattr(SparkSession, "getActiveSession", staticmethod(lambda: sessao))
    return sessao


def test_databricks_usa_a_sessao_ativa_sem_configurar(databricks):
    assert get_spark(app_name="outro") is databricks
    assert databricks.conf == {}


def test_databricks_sem_sessao_ativa_pega_a_do_builder(databricks, monkeypatch):
    monkeypatch.setattr(SparkSession, "getActiveSession", staticmethod(lambda: None))

    class BuilderFalso:
        def getOrCreate(self):
            return databricks

    monkeypatch.setattr(SparkSession, "builder", BuilderFalso())

    assert get_spark() is databricks


def test_databricks_aceita_so_shuffle_partitions(databricks):
    get_spark(conf={"spark.sql.shuffle.partitions": "8"})
    assert databricks.conf == {"spark.sql.shuffle.partitions": "8"}

    with pytest.raises(ValueError, match="snapshotPartitions"):
        get_spark(
            conf={
                "spark.sql.shuffle.partitions": "8",
                "spark.databricks.delta.snapshotPartitions": "2",
            }
        )


def test_databricks_recusa_pacote_maven(databricks):
    with pytest.raises(ValueError, match="Maven"):
        get_spark(pacotes=["org.postgresql:postgresql:42.7.14"])


class DataFrameFalso:
    def __init__(self):
        self.chamadas = []

    def localCheckpoint(self):
        self.chamadas.append("localCheckpoint")
        return "checkpoint"

    def cache(self):
        self.chamadas.append("cache")
        return self

    def count(self):
        self.chamadas.append("count")
        return 1


@pytest.mark.parametrize("modo", [None, "cache", "qualquer"])
def test_materializar_no_local_e_sempre_local_checkpoint(monkeypatch, modo):
    monkeypatch.setenv("BRASILEIRAO_SPARK_ENV", "local")
    if modo is None:
        monkeypatch.delenv("BRASILEIRAO_MATERIALIZAR", raising=False)
    else:
        monkeypatch.setenv("BRASILEIRAO_MATERIALIZAR", modo)
    df = DataFrameFalso()

    assert materializar(df) == "checkpoint"
    assert df.chamadas == ["localCheckpoint"]


def test_materializar_no_databricks_usa_local_checkpoint_por_padrao(monkeypatch):
    monkeypatch.setenv("BRASILEIRAO_SPARK_ENV", "databricks")
    monkeypatch.delenv("BRASILEIRAO_MATERIALIZAR", raising=False)
    df = DataFrameFalso()

    assert materializar(df) == "checkpoint"
    assert df.chamadas == ["localCheckpoint"]


def test_materializar_no_databricks_com_cache_calcula_na_hora(monkeypatch):
    monkeypatch.setenv("BRASILEIRAO_SPARK_ENV", "databricks")
    monkeypatch.setenv("BRASILEIRAO_MATERIALIZAR", "cache")
    df = DataFrameFalso()

    assert materializar(df) is df
    assert df.chamadas == ["cache", "count"]


def test_materializar_no_databricks_recusa_modo_desconhecido(monkeypatch):
    monkeypatch.setenv("BRASILEIRAO_SPARK_ENV", "databricks")
    monkeypatch.setenv("BRASILEIRAO_MATERIALIZAR", "disco")

    with pytest.raises(ValueError, match="disco"):
        materializar(DataFrameFalso())


def test_importar_o_pipeline_nao_puxa_o_serving():
    """No Databricks não há psycopg: `carga` e `exportar` só entram no próprio subcomando."""
    codigo = (
        "import sys, brasileirao.pipeline\n"
        "proibidos = ['psycopg', 'brasileirao.serving', 'brasileirao.aggregate.exportar']\n"
        "print(','.join(m for m in proibidos if m in sys.modules))"
    )
    saida = subprocess.run(
        [sys.executable, "-c", codigo], capture_output=True, text=True, check=True
    ).stdout.strip()

    assert saida == ""


def test_wheel_nao_declara_spark():
    """O Databricks já traz o Spark; o wheel não pode pedir pyspark nem delta-spark."""
    raiz = Path(__file__).parents[2]
    projeto = tomllib.loads((raiz / "pyproject.toml").read_text())
    nomes = {
        re.split(r"[\[<>=~!; ]", dep, maxsplit=1)[0] for dep in projeto["project"]["dependencies"]
    }

    assert not nomes & {"pyspark", "delta-spark"}
    assert {"pyspark", "delta-spark"} <= {
        re.split(r"[\[<>=~!; ]", dep, maxsplit=1)[0]
        for dep in projeto["dependency-groups"]["spark"]
    }
