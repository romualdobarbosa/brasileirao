"""Configuração por variável de ambiente. Todo caminho de dado sai daqui."""

import os

DATA_ROOT_PADRAO = "./data"
SPARK_ENV_PADRAO = "local"
CAMADAS = ("raw", "bronze", "silver")


def data_root() -> str:
    """Raiz das camadas. String, não Path, para aceitar `s3://` na fase AWS."""
    return os.environ.get("BRASILEIRAO_DATA_ROOT", DATA_ROOT_PADRAO).rstrip("/")


def spark_env() -> str:
    return os.environ.get("BRASILEIRAO_SPARK_ENV", SPARK_ENV_PADRAO)


def caminho_camada(camada: str) -> str:
    if camada not in CAMADAS:
        raise ValueError(f"Camada desconhecida: {camada!r}. Use uma de: {', '.join(CAMADAS)}")
    return f"{data_root()}/{camada}"


def caminho_tabela(camada: str, tabela: str) -> str:
    return f"{caminho_camada(camada)}/{tabela}"
