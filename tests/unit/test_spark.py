import pytest

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
