from brasileirao.aggregate.classificacao import calcular_classificacao
from brasileirao.aggregate.ranking import calcular_ranking

from .conftest import MINICAMPEONATO

COLUNAS = [
    ("posicao", "int"),
    ("clube", "string"),
    ("temporadas", "int"),
    ("primeira_temporada", "int"),
    ("ultima_temporada", "int"),
    ("jogos", "int"),
    ("vitorias", "int"),
    ("empates", "int"),
    ("derrotas", "int"),
    ("gols_pro", "int"),
    ("gols_contra", "int"),
    ("saldo", "int"),
    ("pontos", "int"),
    ("aproveitamento", "double"),
    ("titulos", "int"),
]


def _troca(partida, de, para):
    return tuple(para if campo == de else campo for campo in partida)


def _classificacao(silver_partidas, ajustes):
    """2019 é o minicampeonato com o ajuste do Atlas; em 2020 o Eco entra no lugar do Delta."""
    em_2020 = [_troca(p, "Delta", "Eco") for p in MINICAMPEONATO]
    partidas = silver_partidas().unionByName(silver_partidas(em_2020, temporada=2020))
    return calcular_classificacao(partidas, ajustes)


def test_ranking_de_duas_temporadas(silver_partidas, ajustes):
    df = calcular_ranking(_classificacao(silver_partidas, ajustes))

    assert df.dtypes == COLUNAS
    # Campeões: Bravo em 2019 (o ajuste derruba o Atlas) e Atlas em 2020.
    # Bravo e Cometa empatam em pontos e as vitórias decidem; Delta e Eco empatam em tudo
    # e o nome decide.
    assert [tuple(r) for r in df.orderBy("posicao").collect()] == [
        (1, "Atlas", 2, 2019, 2020, 12, 8, 4, 0, 22, 8, 14, 20, 0.7778, 1),
        (2, "Bravo", 2, 2019, 2020, 12, 4, 2, 6, 4, 12, -8, 14, 0.3889, 1),
        (3, "Cometa", 2, 2019, 2020, 12, 2, 8, 2, 12, 10, 2, 14, 0.3889, 0),
        (4, "Delta", 1, 2019, 2019, 6, 0, 3, 3, 2, 6, -4, 3, 0.1667, 0),
        (5, "Eco", 1, 2020, 2020, 6, 0, 3, 3, 2, 6, -4, 3, 0.1667, 0),
    ]


def test_pontos_com_ajuste_e_aproveitamento_sem(silver_partidas, ajustes, sem_ajustes):
    com = {
        r["clube"]: r for r in calcular_ranking(_classificacao(silver_partidas, ajustes)).collect()
    }
    sem = {
        r["clube"]: r
        for r in calcular_ranking(_classificacao(silver_partidas, sem_ajustes)).collect()
    }

    assert sem["Atlas"]["pontos"] - com["Atlas"]["pontos"] == 8
    assert com["Atlas"]["aproveitamento"] == sem["Atlas"]["aproveitamento"]
    assert sem["Atlas"]["titulos"] == 2
