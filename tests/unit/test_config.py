import pytest

from brasileirao import config


def test_data_root_padrao(monkeypatch):
    monkeypatch.delenv("BRASILEIRAO_DATA_ROOT", raising=False)
    assert config.data_root() == "./data"
    assert config.caminho_tabela("bronze", "partidas") == "./data/bronze/partidas"


def test_data_root_por_env_sem_barra_final(monkeypatch):
    monkeypatch.setenv("BRASILEIRAO_DATA_ROOT", "s3://bucket/lake/")
    assert config.caminho_camada("raw") == "s3://bucket/lake/raw"
    assert config.caminho_tabela("silver", "gols") == "s3://bucket/lake/silver/gols"


def test_camada_desconhecida():
    with pytest.raises(ValueError, match="gold"):
        config.caminho_camada("gold")


def test_spark_env(monkeypatch):
    monkeypatch.delenv("BRASILEIRAO_SPARK_ENV", raising=False)
    assert config.spark_env() == "local"
    monkeypatch.setenv("BRASILEIRAO_SPARK_ENV", "emr")
    assert config.spark_env() == "emr"
