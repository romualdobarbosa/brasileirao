import pytest

from brasileirao.aggregate.gols_por_minuto import FAIXAS, calcular_gols_por_minuto
from brasileirao.aggregate.qualidade import checar_gols_por_minuto

COLUNAS = [
    ("temporada", "int"),
    ("faixa", "string"),
    ("lado", "string"),
    ("gols", "int"),
    ("pct_gols", "double"),
]


def _gols(df):
    return {(r["faixa"], r["lado"]): r["gols"] for r in df.collect() if r["gols"]}


def test_uma_linha_por_faixa_e_lado(silver_gols, silver_partidas):
    gols = silver_gols()

    df = calcular_gols_por_minuto(gols, silver_partidas())

    assert df.dtypes == COLUNAS
    assert df.count() == len(FAIXAS) * 2
    assert _gols(df) == {
        ("0-15", "mandante"): 2,
        ("16-30", "mandante"): 2,
        ("31-45+", "mandante"): 1,
        ("31-45+", "visitante"): 1,
        ("46-60", "mandante"): 1,
        ("46-60", "visitante"): 1,
        ("61-75", "mandante"): 1,
        ("76-90+", "mandante"): 3,
    }
    assert checar_gols_por_minuto(df, gols) == []


def test_faixa_pelo_minuto(silver_gols, silver_partidas):
    # Um gol do Atlas, mandante, em cada faixa de borda; o acréscimo não muda a faixa.
    gols = silver_gols(
        [
            (1, "Atlas", "Ari", 0, None, None),
            (1, "Atlas", "Ari", 45, 2, None),
            (5, "Atlas", "Ari", 46, None, None),
            (5, "Atlas", "Ari", 90, 3, None),
        ]
    )

    assert _gols(calcular_gols_por_minuto(gols, silver_partidas())) == {
        ("0-15", "mandante"): 1,
        ("31-45+", "mandante"): 1,
        ("46-60", "mandante"): 1,
        ("76-90+", "mandante"): 1,
    }


def test_gol_contra_vai_para_o_lado_do_beneficiado(silver_gols, silver_partidas):
    # Dino é do Delta, visitante, mas o gol contra é do Atlas, mandante.
    gols = silver_gols([(5, "Atlas", "Dino", 30, None, "Gol Contra")])

    assert _gols(calcular_gols_por_minuto(gols, silver_partidas())) == {("16-30", "mandante"): 1}


def test_pct_gols_soma_1_por_temporada(silver_gols, silver_partidas):
    gols = silver_gols().unionByName(silver_gols(temporada=2020))
    partidas = silver_partidas().unionByName(silver_partidas(temporada=2020))

    df = calcular_gols_por_minuto(gols, partidas)

    somas = df.groupBy("temporada").sum("pct_gols").collect()
    assert sorted(r["temporada"] for r in somas) == [2019, 2020]
    assert all(r["sum(pct_gols)"] == pytest.approx(1, abs=1e-3) for r in somas)
    assert df.where("faixa = '0-15' and lado = 'mandante'").first()["pct_gols"] == 0.1667


def test_checagem_pega_gol_perdido(silver_gols, silver_partidas):
    gols = silver_gols()
    # Gol de uma partida que não está na silver de partidas some no join.
    com_gol_orfao = gols.unionByName(silver_gols([(99, "Atlas", "Ari", 10, None, None)]))

    df = calcular_gols_por_minuto(com_gol_orfao, silver_partidas())

    assert checar_gols_por_minuto(df, com_gol_orfao) == [
        "gols_por_minuto: total de gols ≠ silver/gols: 2019: 12 na tabela, 13 na silver"
    ]
