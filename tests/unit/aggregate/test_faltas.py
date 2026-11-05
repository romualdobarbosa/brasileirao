import pytest

from brasileirao.aggregate.faltas import calcular_faltas_clubes, checar_faltas_clubes

from .conftest import SCHEMA_ESTATISTICAS, _fabrica_filha

COLUNAS = [
    ("temporada", "int"),
    ("clube", "string"),
    ("jogos_com_dado", "int"),
    ("faltas", "int"),
    ("faltas_por_jogo", "double"),
]
# (partida_id, clube, faltas)
FALTAS = [
    (1, "Atlas", 10),
    (1, "Bravo", 15),
    (2, "Atlas", 11),
    (2, "Cometa", 20),
    (3, "Bravo", None),  # linha sem o dado: não conta em `jogos_com_dado`
]


@pytest.fixture
def silver_faltas(spark):
    fabricar = _fabrica_filha(spark, SCHEMA_ESTATISTICAS, ["partida_id", "clube", "faltas"])
    return lambda linhas=FALTAS, temporada=2019: fabricar(linhas, temporada)


def _por_clube(df):
    return {r["clube"]: tuple(r)[2:] for r in df.collect()}


def test_soma_e_media_nos_jogos_com_dado(silver_faltas):
    estatisticas = silver_faltas()

    df = calcular_faltas_clubes(estatisticas)

    assert df.dtypes == COLUNAS
    assert _por_clube(df) == {
        "Atlas": (2, 21, 10.5),
        "Bravo": (1, 15, 15.0),
        "Cometa": (1, 20, 20.0),
    }
    assert checar_faltas_clubes(df, estatisticas) == []


def test_media_com_duas_casas(silver_faltas):
    estatisticas = silver_faltas([(1, "Atlas", 10), (2, "Atlas", 10), (3, "Atlas", 11)])

    assert _por_clube(calcular_faltas_clubes(estatisticas)) == {"Atlas": (3, 31, 10.33)}


def test_2014_fora_e_2015_dentro(silver_faltas):
    estatisticas = silver_faltas(temporada=2014).unionByName(silver_faltas(temporada=2015))

    df = calcular_faltas_clubes(estatisticas)

    assert {r["temporada"] for r in df.collect()} == {2015}
    assert checar_faltas_clubes(df, estatisticas) == []


def test_temporada_sem_dado_nao_gera_linha(silver_faltas):
    estatisticas = silver_faltas().unionByName(
        silver_faltas([(1, "Atlas", None), (1, "Bravo", None)], temporada=2024)
    )

    df = calcular_faltas_clubes(estatisticas)

    assert {r["temporada"] for r in df.collect()} == {2019}
    assert checar_faltas_clubes(df, estatisticas) == []


def test_checagem_pega_total_que_nao_fecha(silver_faltas):
    estatisticas = silver_faltas([(1, "Atlas", 10)])
    outra = silver_faltas([(1, "Atlas", 10), (2, "Atlas", 12)])

    df = calcular_faltas_clubes(estatisticas)

    assert checar_faltas_clubes(df, outra) == [
        "faltas_clubes: totais ≠ silver/estatisticas: "
        "2019: 1 jogos e 10 faltas na tabela, 2 jogos e 22 na silver"
    ]
