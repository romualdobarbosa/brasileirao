import shutil
from datetime import datetime
from pathlib import Path

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
            "spark.databricks.delta.snapshotPartitions": "2",
        },
    )
    yield sessao
    sessao.stop()


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    """Aponta BRASILEIRAO_DATA_ROOT para um diretório temporário do teste."""
    monkeypatch.setenv("BRASILEIRAO_DATA_ROOT", str(tmp_path))
    return tmp_path


FIXTURES_RAW = Path(__file__).parent / "fixtures" / "raw"
COLUNAS_FONTE_PARTIDAS = (
    "ID,rodata,data,hora,mandante,visitante,formacao_mandante,formacao_visitante,"
    "tecnico_mandante,tecnico_visitante,vencedor,arena,mandante_Placar,visitante_Placar,"
    "mandante_Estado,visitante_Estado,arrecadacao"
).split(",")


@pytest.fixture
def raw(data_root):
    """`<data_root>/raw` com os CSVs de `tests/fixtures/raw`. Devolve o diretório."""
    destino = data_root / "raw"
    shutil.copytree(FIXTURES_RAW, destino)
    return destino


@pytest.fixture
def bronze_partidas(spark):
    """Fábrica de DataFrames no formato de `bronze/partidas`.

    Cada argumento é um dict só com as colunas que o teste quer fixar; o resto vem de uma
    partida válida de 2019.
    """
    padrao = {
        "ID": "1",
        "rodata": "1",
        "data": "27/04/2019",
        "hora": "16:00",
        "mandante": "Sao Paulo",
        "visitante": "Gremio",
        "formacao_mandante": "4-2-3-1",
        "formacao_visitante": "4-3-3",
        "tecnico_mandante": "A. Stival",
        "tecnico_visitante": "R. Portaluppi",
        "vencedor": "Sao Paulo",
        "arena": "Morumbi",
        "mandante_Placar": "2",
        "visitante_Placar": "0",
        "mandante_Estado": "SP",
        "visitante_Estado": "RS",
        "arrecadacao": None,
        "_ingested_at": datetime(2026, 10, 27, 12, 0),
        "_source_file": "campeonato-brasileiro-full.csv",
        "temporada": 2019,
    }
    schema = (
        ", ".join(f"{c} string" for c in COLUNAS_FONTE_PARTIDAS)
        + ", _ingested_at timestamp, _source_file string, temporada int"
    )

    def fabricar(*linhas: dict):
        return spark.createDataFrame([{**padrao, **linha} for linha in linhas], schema)

    return fabricar


@pytest.fixture
def mapa_times(spark):
    """Mapa pequeno com um alias ("Gremio FBPA" → "Grêmio")."""
    return spark.createDataFrame(
        [
            ("Sao Paulo", "São Paulo", "SP"),
            ("Gremio", "Grêmio", "RS"),
            ("Gremio FBPA", "Grêmio", "RS"),
            ("Santos", "Santos", "SP"),
        ],
        "nome_fonte string, nome_canonico string, uf string",
    )


COLUNAS_FONTE_FILHAS = {
    "gols": "partida_id,rodata,clube,atleta,minuto,tipo_de_gol",
    "cartoes": "partida_id,rodata,clube,cartao,atleta,num_camisa,posicao,minuto",
    "estatisticas": (
        "partida_id,rodata,clube,chutes,chutes_no_alvo,posse_de_bola,passes,precisao_passes,"
        "faltas,cartao_amarelo,cartao_vermelho,impedimentos,escanteios"
    ),
}
PADRAO_FILHAS = {
    "gols": {"atleta": "Pato", "minuto": "31", "tipo_de_gol": ""},
    "cartoes": {
        "cartao": "Amarelo",
        "atleta": "Arboleda",
        "num_camisa": "5",
        "posicao": "Zagueiro",
        "minuto": "44",
    },
    "estatisticas": {
        "chutes": "15",
        "chutes_no_alvo": "6",
        "posse_de_bola": "55%",
        "passes": "480",
        "precisao_passes": "86%",
        "faltas": "12",
        "cartao_amarelo": "1",
        "cartao_vermelho": "0",
        "impedimentos": "2",
        "escanteios": "7",
    },
}


@pytest.fixture
def bronze_filha(spark):
    """Fábrica de DataFrames no formato de `bronze/gols`, `bronze/cartoes` ou
    `bronze/estatisticas`. Como em `bronze_partidas`, cada dict só fixa o que o teste quer."""

    def fabricar(entidade: str, *linhas: dict):
        colunas = COLUNAS_FONTE_FILHAS[entidade].split(",")
        padrao = {
            "partida_id": "1",
            "rodata": "1",
            "clube": "Sao Paulo",
            **PADRAO_FILHAS[entidade],
            "_ingested_at": datetime(2026, 10, 27, 12, 0),
            "_source_file": "campeonato-brasileiro.csv",
            "temporada": 2019,
        }
        schema = (
            ", ".join(f"{c} string" for c in colunas)
            + ", _ingested_at timestamp, _source_file string, temporada int"
        )
        return spark.createDataFrame([{**padrao, **linha} for linha in linhas], schema)

    return fabricar
