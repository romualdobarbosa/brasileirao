from datetime import date

from pyspark.sql import functions as F

from brasileirao.aggregate.gols_decisivos import calcular_gols_decisivos, checar_gols_decisivos

COLUNAS = [
    ("temporada", "int"),
    ("clube", "string"),
    ("jogos", "int"),
    ("gols_85", "int"),
    ("gols_acrescimo", "int"),
    ("pontos_ganhos_85", "int"),
    ("pontos_perdidos_85", "int"),
    ("pontos_ganhos_acrescimo", "int"),
    ("pontos_perdidos_acrescimo", "int"),
]
DATA = date(2019, 5, 1)
# Só os gols da faixa e um de antes dela: o placar final vem de `silver/partidas`.
GOLS_NO_FIM = [
    (1, "Atlas", "Ari", 10, None, None),  # 2 × 0 do Atlas: já ganhava antes da faixa
    (1, "Atlas", "Ari", 90, 1, None),
    (5, "Delta", "Dino", 87, None, None),  # 3 × 1 do Atlas: gol de honra não muda pontos
    (9, "Cometa", "Edu", 90, 3, None),  # 2 × 2: o Cometa empata no acréscimo
    (12, "Cometa", "Edu", 86, None, None),  # 2 × 0 do Cometa: 0 × 0 até os 85
    (12, "Cometa", "Edu", 90, None, None),  # minuto 90 sem acréscimo: só na faixa dos 85
    (7, "Atlas", "Ari", 88, None, None),  # 2 × 0 do Atlas: 0 × 0 até os 85
    (7, "Atlas", "Ari", 90, 2, None),
]


def _por_clube(df):
    return {r["clube"]: tuple(r)[2:] for r in df.collect()}


def test_gols_e_pontos_de_cada_faixa(silver_partidas, silver_gols):
    gols = silver_gols(GOLS_NO_FIM)

    df = calcular_gols_decisivos(gols, silver_partidas())

    assert df.dtypes == COLUNAS
    # (jogos, gols_85, gols_acrescimo, ganhos_85, perdidos_85, ganhos_acr, perdidos_acr)
    assert _por_clube(df) == {
        "Atlas": (6, 3, 2, 2, 2, 0, 2),
        "Bravo": (6, 0, 0, 0, 2, 0, 0),
        "Cometa": (6, 3, 1, 3, 0, 1, 0),
        "Delta": (6, 1, 0, 0, 0, 0, 0),
    }


def test_clube_sem_gol_na_faixa_e_jogo_sem_gols_contam_em_jogos(silver_partidas, silver_gols):
    # Bravo e Delta empatam 0 × 0 e nenhum dos 4 clubes faz gol no fim.
    df = calcular_gols_decisivos(silver_gols([]), silver_partidas())

    assert _por_clube(df) == {
        clube: (6, 0, 0, 0, 0, 0, 0) for clube in ["Atlas", "Bravo", "Cometa", "Delta"]
    }


def test_bordas_das_faixas(silver_partidas, silver_gols):
    partidas = [(1, 1, DATA, "Atlas", "Bravo", 4, 0)]
    gols = silver_gols(
        [
            (1, "Atlas", "Ari", 45, 2, None),  # acréscimo do 1º tempo: fora das duas faixas
            (1, "Atlas", "Ari", 84, None, None),  # fora
            (1, "Atlas", "Ari", 85, None, None),  # só nos 85
            (1, "Atlas", "Ari", 90, 1, None),  # nas duas
        ]
    )

    linha = (
        calcular_gols_decisivos(gols, silver_partidas(partidas)).where("clube = 'Atlas'").first()
    )

    assert (linha["gols_85"], linha["gols_acrescimo"]) == (2, 1)


def test_gol_contra_vale_para_o_beneficiado(silver_partidas, silver_gols):
    # Ari é do Atlas e faz contra aos 89: o Bravo empata e o Atlas perde 2 pontos.
    partidas = [(1, 1, DATA, "Atlas", "Bravo", 1, 1)]
    gols = silver_gols(
        [
            (1, "Atlas", "Ari", 10, None, None),
            (1, "Bravo", "Ari", 89, None, "Gol Contra"),
        ]
    )

    df = calcular_gols_decisivos(gols, silver_partidas(partidas))

    assert _por_clube(df) == {
        "Atlas": (1, 0, 0, 0, 2, 0, 0),
        "Bravo": (1, 1, 0, 1, 0, 0, 0),
    }


def test_virada_nos_85_da_tres_pontos_e_tira_tres(silver_partidas, silver_gols):
    partidas = [(1, 1, DATA, "Atlas", "Bravo", 2, 1)]
    gols = silver_gols(
        [
            (1, "Bravo", "Bia", 10, None, None),
            (1, "Atlas", "Ari", 86, None, None),
            (1, "Atlas", "Ari", 90, 4, None),
        ]
    )

    df = calcular_gols_decisivos(gols, silver_partidas(partidas))

    assert _por_clube(df) == {
        "Atlas": (1, 2, 1, 3, 0, 2, 0),
        "Bravo": (1, 0, 0, 0, 3, 0, 1),  # no acréscimo, o Bravo só perde o empate
    }


def test_gols_dos_dois_lados_na_faixa(silver_partidas, silver_gols):
    # 1 × 1 até os 85 e 2 × 2 no fim: os pontos não mudam, mesmo com gols na faixa.
    partidas = [(1, 1, DATA, "Atlas", "Bravo", 2, 2)]
    gols = silver_gols(
        [
            (1, "Atlas", "Ari", 10, None, None),
            (1, "Bravo", "Bia", 20, None, None),
            (1, "Atlas", "Ari", 86, None, None),
            (1, "Bravo", "Bia", 88, None, None),
        ]
    )

    df = calcular_gols_decisivos(gols, silver_partidas(partidas))

    assert _por_clube(df) == {
        "Atlas": (1, 1, 0, 0, 0, 0, 0),
        "Bravo": (1, 1, 0, 0, 0, 0, 0),
    }


def test_temporada_antes_de_2014_fica_de_fora(silver_partidas, silver_gols):
    partidas = silver_partidas().unionByName(silver_partidas(temporada=2013))
    gols = silver_gols([])

    df = calcular_gols_decisivos(gols, partidas)

    assert {r["temporada"] for r in df.collect()} == {2019}
    assert checar_gols_decisivos(df, gols, partidas) == []


def test_2014_entra(silver_partidas, silver_gols):
    df = calcular_gols_decisivos(silver_gols([]), silver_partidas(temporada=2014))

    assert {r["temporada"] for r in df.collect()} == {2014}


def test_checagem_passa_com_totais_certos(silver_partidas, silver_gols):
    gols = silver_gols(GOLS_NO_FIM)
    partidas = silver_partidas()

    df = calcular_gols_decisivos(gols, partidas)

    assert checar_gols_decisivos(df, gols, partidas) == []


def test_checagem_pega_gol_de_partida_desconhecida(silver_partidas, silver_gols):
    # O gol da partida 99 está na silver, mas some no join com as partidas.
    gols = silver_gols([(99, "Atlas", "Ari", 90, 1, None)])
    partidas = silver_partidas()

    df = calcular_gols_decisivos(gols, partidas)

    assert checar_gols_decisivos(df, gols, partidas) == [
        "gols_decisivos: totais ≠ silver: 2019: jogos 24 na tabela, 24 na silver, "
        "gols_85 0 na tabela, 1 na silver, gols_acrescimo 0 na tabela, 1 na silver"
    ]


def test_checagem_pega_jogo_a_mais(silver_partidas, silver_gols):
    gols = silver_gols([])
    partidas = silver_partidas()
    com_jogo_a_mais = calcular_gols_decisivos(gols, partidas).withColumn(
        "jogos", F.col("jogos") + 1
    )

    assert checar_gols_decisivos(com_jogo_a_mais, gols, partidas) == [
        "gols_decisivos: totais ≠ silver: 2019: jogos 28 na tabela, 24 na silver, "
        "gols_85 0 na tabela, 0 na silver, gols_acrescimo 0 na tabela, 0 na silver"
    ]
