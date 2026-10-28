from brasileirao.aggregate.referencia import carregar_ajustes
from brasileirao.transform.silver import carregar_mapa_times

# Pontos finais oficiais menos os de campo (3/1/0) da silver, conferidos na spec.
TOTAIS_SPEC = {
    (2003, "Ponte Preta"): -1,
    (2003, "Paysandu"): -8,
    (2003, "Internacional"): 2,
    (2003, "Juventude"): 3,
    (2003, "São Caetano"): 3,
    (2003, "Corinthians"): 2,
    (2003, "Fluminense"): 2,
    (2004, "São Caetano"): -24,
    (2010, "Grêmio Prudente"): -3,
    (2013, "Portuguesa"): -4,
    (2013, "Flamengo"): -4,
    (2016, "Santa Cruz"): -3,
    (2018, "Sport"): -3,
}


def test_schema(spark):
    ajustes = carregar_ajustes(spark)

    assert ajustes.dtypes == [
        ("temporada", "int"),
        ("clube", "string"),
        ("pontos", "int"),
        ("motivo", "string"),
        ("fonte", "string"),
    ]
    assert ajustes.count() == 14


def test_clubes_canonicos(spark):
    canonicos = {r["nome_canonico"] for r in carregar_mapa_times(spark).collect()}

    clubes = {r["clube"] for r in carregar_ajustes(spark).collect()}

    assert clubes <= canonicos


def test_soma_por_temporada_e_clube_bate_com_a_spec(spark):
    linhas = carregar_ajustes(spark).groupBy("temporada", "clube").sum("pontos").collect()

    assert {(r["temporada"], r["clube"]): r["sum(pontos)"] for r in linhas} == TOTAIS_SPEC


def test_fonte_e_a_pagina_da_temporada(spark):
    for r in carregar_ajustes(spark).collect():
        assert r["fonte"] == (
            "https://pt.wikipedia.org/wiki/"
            f"Campeonato_Brasileiro_de_Futebol_de_{r['temporada']}_-_Série_A"
        )
        assert r["motivo"]
