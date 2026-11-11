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


def test_camada_gold(monkeypatch):
    monkeypatch.setenv("BRASILEIRAO_DATA_ROOT", "/lake")
    assert config.caminho_tabela("gold", "x") == "/lake/gold/x"


def test_camada_desconhecida():
    with pytest.raises(ValueError, match="platina"):
        config.caminho_camada("platina")


def test_spark_env(monkeypatch):
    monkeypatch.delenv("BRASILEIRAO_SPARK_ENV", raising=False)
    assert config.spark_env() == "local"
    monkeypatch.setenv("BRASILEIRAO_SPARK_ENV", "aws")
    assert config.spark_env() == "aws"


def _limpar_pg(monkeypatch):
    for variavel in (*config.PG_PADROES, "BRASILEIRAO_PG_SENHA"):
        monkeypatch.delenv(variavel, raising=False)


def test_jdbc_com_padroes(monkeypatch):
    _limpar_pg(monkeypatch)
    monkeypatch.setenv("BRASILEIRAO_PG_SENHA", "s3gr3do")

    url, propriedades = config.jdbc_postgres()

    assert url == "jdbc:postgresql://localhost:5432/brasileirao"
    assert propriedades == {
        "user": "carga",
        "password": "s3gr3do",
        "driver": "org.postgresql.Driver",
    }


def test_jdbc_pelo_ambiente(monkeypatch):
    monkeypatch.setenv("BRASILEIRAO_PG_HOST", "vm")
    monkeypatch.setenv("BRASILEIRAO_PG_PORT", "15432")
    monkeypatch.setenv("BRASILEIRAO_PG_DB", "outro")
    monkeypatch.setenv("BRASILEIRAO_PG_USUARIO", "fulano")
    monkeypatch.setenv("BRASILEIRAO_PG_SENHA", "x")

    url, propriedades = config.jdbc_postgres()

    assert url == "jdbc:postgresql://vm:15432/outro"
    assert propriedades["user"] == "fulano"
    assert config.pg_porta() == 15432


def test_sem_senha_diz_qual_variavel_falta(monkeypatch):
    _limpar_pg(monkeypatch)
    with pytest.raises(config.VariavelAusenteError, match="BRASILEIRAO_PG_SENHA"):
        config.jdbc_postgres()


def test_senha_vazia_conta_como_ausente(monkeypatch):
    monkeypatch.setenv("BRASILEIRAO_PG_SENHA", "")
    with pytest.raises(config.VariavelAusenteError, match="BRASILEIRAO_PG_SENHA"):
        config.pg_senha()


def test_porta_invalida(monkeypatch):
    monkeypatch.setenv("BRASILEIRAO_PG_PORT", "cinco")
    with pytest.raises(ValueError, match="BRASILEIRAO_PG_PORT"):
        config.pg_porta()
