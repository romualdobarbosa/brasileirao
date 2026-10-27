import pytest

from brasileirao.utils.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    sessao = get_spark(
        app_name="brasileirao-testes",
        conf={
            "spark.master": "local[2]",
            "spark.sql.shuffle.partitions": "2",
            "spark.ui.enabled": "false",
        },
    )
    yield sessao
    sessao.stop()


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    """Aponta BRASILEIRAO_DATA_ROOT para um diretório temporário do teste."""
    monkeypatch.setenv("BRASILEIRAO_DATA_ROOT", str(tmp_path))
    return tmp_path
