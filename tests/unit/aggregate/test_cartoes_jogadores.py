from brasileirao.aggregate.cartoes_jogadores import (
    calcular_cartoes_jogadores,
    checar_cartoes_jogadores,
)

from .conftest import CARTOES

COLUNAS = [
    ("temporada", "int"),
    ("atleta", "string"),
    ("clube", "string"),
    ("amarelos", "int"),
    ("vermelhos", "int"),
]


def _por_atleta(df):
    return {(r["atleta"], r["clube"]): (r["amarelos"], r["vermelhos"]) for r in df.collect()}


def test_conta_cartoes_por_atleta_e_clube(silver_cartoes):
    cartoes = silver_cartoes()

    df = calcular_cartoes_jogadores(cartoes)

    assert df.dtypes == COLUNAS
    assert _por_atleta(df) == {
        ("Bia", "Bravo"): (1, 1),
        ("Ari", "Atlas"): (1, 0),
        ("Caio", "Cometa"): (1, 0),
        ("Edu", "Cometa"): (1, 1),
    }
    assert checar_cartoes_jogadores(df, cartoes) == []


def test_quem_troca_de_clube_fica_com_uma_linha_em_cada(silver_cartoes):
    cartoes = silver_cartoes([(1, "Atlas", "Amarelo", "Ari"), (2, "Bravo", "Vermelho", "Ari")])

    assert _por_atleta(calcular_cartoes_jogadores(cartoes)) == {
        ("Ari", "Atlas"): (1, 0),
        ("Ari", "Bravo"): (0, 1),
    }


def test_temporadas_separadas(silver_cartoes):
    cartoes = silver_cartoes().unionByName(silver_cartoes(temporada=2020))

    df = calcular_cartoes_jogadores(cartoes)

    assert df.where("temporada = 2020").count() == df.where("temporada = 2019").count() == 4


def test_linha_sem_atleta_fica_de_fora(silver_cartoes):
    cartoes = silver_cartoes([*CARTOES, (1, "Bravo", "Amarelo", None)])

    df = calcular_cartoes_jogadores(cartoes)

    assert df.where("atleta is null").count() == 0
    assert df.groupBy().sum("amarelos").first()[0] == 4
    assert checar_cartoes_jogadores(df, cartoes) == []


def test_checagem_pega_tipo_de_cartao_desconhecido(silver_cartoes):
    cartoes = silver_cartoes([(1, "Bravo", "Azul", "Bia")])

    df = calcular_cartoes_jogadores(cartoes)

    assert checar_cartoes_jogadores(df, cartoes) == [
        "cartoes_jogadores: total de cartões ≠ silver/cartoes: 2019: 0 na tabela, 1 na silver"
    ]
