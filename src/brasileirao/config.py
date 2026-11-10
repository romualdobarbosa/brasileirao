"""Configuração por variável de ambiente. Todo caminho de dado sai daqui."""

import os

DATA_ROOT_PADRAO = "./data"
SPARK_ENV_PADRAO = "local"
MATERIALIZACAO_PADRAO = "local_checkpoint"
CAMADAS = ("raw", "bronze", "silver", "gold")


def data_root() -> str:
    """Raiz das camadas. String, não Path, para aceitar `s3://` na fase AWS."""
    return os.environ.get("BRASILEIRAO_DATA_ROOT", DATA_ROOT_PADRAO).rstrip("/")


def spark_env() -> str:
    return os.environ.get("BRASILEIRAO_SPARK_ENV", SPARK_ENV_PADRAO)


def materializacao_databricks() -> str:
    """Como `materializar` calcula um DataFrame no Databricks. No local é sempre o mesmo."""
    return os.environ.get("BRASILEIRAO_MATERIALIZAR", MATERIALIZACAO_PADRAO)


def caminho_camada(camada: str) -> str:
    if camada not in CAMADAS:
        raise ValueError(f"Camada desconhecida: {camada!r}. Use uma de: {', '.join(CAMADAS)}")
    return f"{data_root()}/{camada}"


def caminho_tabela(camada: str, tabela: str) -> str:
    return f"{caminho_camada(camada)}/{tabela}"


# Postgres do serving. A senha não tem padrão: vem do .env (fora do git).
PG_PADROES = {
    "BRASILEIRAO_PG_HOST": "localhost",
    "BRASILEIRAO_PG_PORT": "5432",
    "BRASILEIRAO_PG_DB": "brasileirao",
    "BRASILEIRAO_PG_USUARIO": "carga",
}


class VariavelAusenteError(RuntimeError):
    pass


def _pg(variavel: str) -> str:
    valor = os.environ.get(variavel, PG_PADROES.get(variavel))
    if not valor:
        raise VariavelAusenteError(
            f"{variavel} não está definida: copie serving/.env.example para .env e preencha"
        )
    return valor


def pg_host() -> str:
    return _pg("BRASILEIRAO_PG_HOST")


def pg_porta() -> int:
    valor = _pg("BRASILEIRAO_PG_PORT")
    if not valor.isdigit():
        raise ValueError(f"BRASILEIRAO_PG_PORT precisa ser um número: {valor!r}")
    return int(valor)


def pg_banco() -> str:
    return _pg("BRASILEIRAO_PG_DB")


def pg_usuario() -> str:
    return _pg("BRASILEIRAO_PG_USUARIO")


def pg_senha() -> str:
    return _pg("BRASILEIRAO_PG_SENHA")


def jdbc_postgres() -> tuple[str, dict[str, str]]:
    """URL JDBC e propriedades de conexão para `DataFrameWriter.jdbc`."""
    url = f"jdbc:postgresql://{pg_host()}:{pg_porta()}/{pg_banco()}"
    propriedades = {
        "user": pg_usuario(),
        "password": pg_senha(),
        "driver": "org.postgresql.Driver",
    }
    return url, propriedades
