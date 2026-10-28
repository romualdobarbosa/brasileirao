from brasileirao.aggregate.artilharia import calcular_artilharia

COLUNAS = [
    ("temporada", "int"),
    ("posicao", "int"),
    ("atleta", "string"),
    ("clubes", "string"),
    ("gols", "int"),
    ("penaltis", "int"),
]


def test_artilharia_do_minicampeonato(silver_gols):
    df = calcular_artilharia(silver_gols())

    assert df.dtypes == COLUNAS
    # Ari e Caio empatam no topo e dividem a posição 1; o próximo fica em 3.
    # O gol contra do Dino não conta: ele fica só com o gol pelo Delta.
    assert [tuple(r) for r in df.orderBy("posicao", "atleta").collect()] == [
        (2019, 1, "Ari", "Atlas", 3, 1),
        (2019, 1, "Caio", "Bravo, Cometa", 3, 1),
        (2019, 3, "Beto", "Atlas", 2, 0),
        (2019, 3, "Edu", "Cometa", 2, 0),
        (2019, 5, "Dino", "Delta", 1, 0),
    ]


def test_gol_contra_nao_conta_para_ninguem(silver_gols):
    df = calcular_artilharia(silver_gols([(5, "Atlas", "Dino", 30, None, "Gol Contra")]))

    assert df.count() == 0


def test_posicao_por_temporada(silver_gols):
    gols = silver_gols().unionByName(
        silver_gols([(1, "Bravo", "Fábio", 10, None, None)], temporada=2020)
    )

    df = calcular_artilharia(gols)

    assert [tuple(r) for r in df.where("temporada = 2020").collect()] == [
        (2020, 1, "Fábio", "Bravo", 1, 0)
    ]
