import pytest

from brasileirao.utils import spark
from brasileirao.utils.spark import get_spark


def test_ambiente_desconhecido(monkeypatch):
    monkeypatch.setenv("BRASILEIRAO_SPARK_ENV", "marte")
    with pytest.raises(ValueError, match="marte"):
        get_spark()


@pytest.mark.parametrize("ambiente", ["emr", "databricks"])
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
    monkeypatch.setattr(spark, "configure_spark_with_delta_pip", configurar)

    assert get_spark(pacotes=["org.postgresql:postgresql:42.7.14"]) == "sessao"
    assert recebidos["pacotes"] == ["org.postgresql:postgresql:42.7.14"]
