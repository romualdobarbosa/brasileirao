from datetime import date

from pyspark.sql import functions as F

from brasileirao.aggregate.viradas import calcular_viradas, checar_viradas, placar_corrente

COLUNAS = [
    ("partida_id", "int"),
    ("clube", "string"),
    ("temporada", "int"),
    ("rodada", "int"),
    ("data", "date"),
    ("adversario", "string"),
    ("mando", "string"),
    ("gols_pro", "int"),
    ("gols_contra", "int"),
    ("maior_desvantagem", "int"),
]
DATA = date(2019, 5, 1)


def _partida(mandante_gols, visitante_gols, partida_id=1):
    return [(partida_id, 1, DATA, "Atlas", "Bravo", mandante_gols, visitante_gols)]


def _viradas(df):
    return {(r["partida_id"], r["clube"]): r["maior_desvantagem"] for r in df.collect()}


def test_mandante_que_vira(silver_partidas, silver_gols):
    gols = silver_gols(
        [
            (1, "Bravo", "Bia", 20, None, None),
            (1, "Atlas", "Ari", 30, None, None),
            (1, "Atlas", "Ari", 40, None, None),
        ]
    )
    partidas = silver_partidas(_partida(2, 1))

    df = calcular_viradas(gols, partidas)

    assert df.dtypes == COLUNAS
    linha = df.first()
    assert tuple(linha) == (1, "Atlas", 2019, 1, DATA, "Bravo", "mandante", 2, 1, 1)
    assert checar_viradas(df, gols, partidas) == []


def test_visitante_que_vira(silver_partidas, silver_gols):
    gols = silver_gols(
        [
            (1, "Atlas", "Ari", 10, None, None),
            (1, "Bravo", "Bia", 20, None, None),
            (1, "Bravo", "Bia", 30, None, None),
        ]
    )

    linha = calcular_viradas(gols, silver_partidas(_partida(1, 2))).first()

    assert (linha["clube"], linha["adversario"], linha["mando"]) == ("Bravo", "Atlas", "visitante")
    assert (linha["gols_pro"], linha["gols_contra"]) == (2, 1)


def test_maior_desvantagem_e_o_pico_contra_o_clube(silver_partidas, silver_gols):
    gols = silver_gols(
        [
            (1, "Bravo", "Bia", 5, None, None),
            (1, "Bravo", "Bia", 10, None, None),
            (1, "Atlas", "Ari", 20, None, None),
            (1, "Bravo", "Bia", 25, None, None),
            (1, "Atlas", "Ari", 30, None, None),
            (1, "Atlas", "Ari", 40, None, None),
            (1, "Atlas", "Ari", 50, None, None),
            (1, "Atlas", "Ari", 60, None, None),
        ]
    )

    df = calcular_viradas(gols, silver_partidas(_partida(5, 3)))

    assert _viradas(df) == {(1, "Atlas"): 2}


def test_45_mais_2_vem_antes_do_46(silver_partidas, silver_gols):
    # Somando minuto e acréscimo, o 46 viria antes e o Atlas nunca teria estado atrás.
    gols = silver_gols(
        [
            (1, "Atlas", "Ari", 46, None, None),
            (1, "Bravo", "Bia", 45, 2, None),
            (1, "Atlas", "Ari", 60, None, None),
        ]
    )

    assert _viradas(calcular_viradas(gols, silver_partidas(_partida(2, 1)))) == {(1, "Atlas"): 1}


def test_gol_sem_acrescimo_vem_antes_do_acrescimo_no_mesmo_minuto(silver_partidas, silver_gols):
    gols = silver_gols(
        [
            (1, "Atlas", "Ari", 90, 3, None),
            (1, "Atlas", "Ari", 90, 5, None),
            (1, "Bravo", "Bia", 90, None, None),
        ]
    )

    assert _viradas(calcular_viradas(gols, silver_partidas(_partida(2, 1)))) == {(1, "Atlas"): 1}


def test_gol_contra_vale_para_o_beneficiado(silver_partidas, silver_gols):
    # Ari é do Atlas: o gol contra dele põe o Bravo na frente. O de Bia empata para o Atlas.
    gols = silver_gols(
        [
            (1, "Bravo", "Ari", 10, None, "Gol Contra"),
            (1, "Atlas", "Bia", 20, None, "Gol Contra"),
            (1, "Atlas", "Ari", 30, None, None),
        ]
    )

    assert _viradas(calcular_viradas(gols, silver_partidas(_partida(2, 1)))) == {(1, "Atlas"): 1}


def test_quem_nao_esteve_atras_nao_virou(silver_partidas, silver_gols):
    # Empate depois de sair atrás, vitória sem sair atrás e vitória saindo de um empate.
    partidas = [
        (1, 1, DATA, "Atlas", "Bravo", 1, 1),
        (2, 1, DATA, "Atlas", "Cometa", 2, 0),
        (3, 1, DATA, "Atlas", "Delta", 2, 1),
    ]
    gols = silver_gols(
        [
            (1, "Bravo", "Bia", 10, None, None),
            (1, "Atlas", "Ari", 20, None, None),
            (2, "Atlas", "Ari", 10, None, None),
            (2, "Atlas", "Ari", 20, None, None),
            (3, "Atlas", "Ari", 10, None, None),
            (3, "Delta", "Dino", 20, None, None),
            (3, "Atlas", "Ari", 30, None, None),
        ]
    )

    assert calcular_viradas(gols, silver_partidas(partidas)).count() == 0


def test_gols_do_mesmo_instante_entram_juntos(silver_partidas, silver_gols):
    gols = silver_gols(
        [
            (1, "Atlas", "Ari", 10, None, None),
            (1, "Atlas", "Ari", 89, None, None),
            (1, "Bravo", "Bia", 89, None, None),
        ]
    )

    linhas = placar_corrente(gols, silver_partidas(_partida(2, 1))).where("minuto = 89").collect()

    assert {(r["corrente_mandante"], r["corrente_visitante"]) for r in linhas} == {(2, 1)}


def test_placar_corrente_sem_acrescimo_nem_gol_contra(silver_partidas, silver_gols):
    gols = silver_gols(
        [
            (1, "Bravo", "Bia", 10, None, None),
            (1, "Atlas", "Ari", 20, None, None),
            (1, "Atlas", "Ari", 30, None, None),
        ]
    )

    df = placar_corrente(gols, silver_partidas(_partida(2, 1))).orderBy("minuto")

    assert [(r["corrente_mandante"], r["corrente_visitante"]) for r in df.collect()] == [
        (0, 1),
        (1, 1),
        (2, 1),
    ]


def test_checagem_aceita_0_x_0_e_ignora_partida_antes_de_2014(silver_partidas, silver_gols):
    # O 0 × 0 de 2019 não tem gol, e o 3 × 1 de 2013 também não tem, porque a fonte não cobre.
    partidas = silver_partidas(_partida(0, 0)).unionByName(
        silver_partidas(_partida(3, 1, partida_id=2), temporada=2013)
    )
    gols = silver_gols([])

    df = calcular_viradas(gols, partidas)

    assert df.count() == 0
    assert checar_viradas(df, gols, partidas) == []


def test_checagem_pega_placar_que_nao_fecha(silver_partidas, silver_gols):
    gols = silver_gols([(1, "Atlas", "Ari", 10, None, None)])
    partidas = silver_partidas(_partida(2, 1))

    df = calcular_viradas(gols, partidas)

    assert checar_viradas(df, gols, partidas) == [
        "viradas: placar reconstruído ≠ placar da partida: 1"
    ]


def test_checagem_pega_gol_de_partida_desconhecida(silver_partidas, silver_gols):
    gols = silver_gols([(99, "Atlas", "Ari", 10, None, None)])
    partidas = silver_partidas(_partida(0, 0))

    df = calcular_viradas(gols, partidas)

    assert checar_viradas(df, gols, partidas) == [
        "viradas: placar reconstruído ≠ placar da partida: 99"
    ]


def test_checagem_pega_linha_que_nao_e_virada(silver_partidas, silver_gols):
    gols = silver_gols(
        [
            (1, "Bravo", "Bia", 10, None, None),
            (1, "Atlas", "Ari", 20, None, None),
            (1, "Atlas", "Ari", 30, None, None),
        ]
    )
    partidas = silver_partidas(_partida(2, 1))

    df = calcular_viradas(gols, partidas).withColumn("gols_pro", F.col("gols_contra"))

    assert checar_viradas(df, gols, partidas) == ["viradas: linha que não é virada: 1 Atlas"]
