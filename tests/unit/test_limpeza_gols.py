from brasileirao.transform import limpeza

SCHEMA_SILVER = [
    ("partida_id", "int"),
    ("temporada", "int"),
    ("rodada", "int"),
    ("clube", "string"),
    ("atleta", "string"),
    ("minuto", "int"),
    ("acrescimo", "int"),
    ("tipo_de_gol", "string"),
]


def _gols(bronze_filha, *linhas):
    return limpeza.limpar_gols(bronze_filha("gols", *linhas))


def test_minuto_com_acrescimo(bronze_filha):
    df = _gols(bronze_filha, {"partida_id": "1", "minuto": "45+2"}, {"partida_id": "2"})

    minutos = {r["partida_id"]: (r["minuto"], r["acrescimo"]) for r in df.collect()}

    assert minutos == {1: (45, 2), 2: (31, None)}


def test_minuto_vazio_ou_invalido_vira_nulo(bronze_filha):
    df = _gols(bronze_filha, {"partida_id": "1", "minuto": ""}, {"partida_id": "2", "minuto": "x"})

    assert {(r["minuto"], r["acrescimo"]) for r in df.collect()} == {(None, None)}


def test_tipo_de_gol_vazio_vira_nulo(bronze_filha):
    df = _gols(
        bronze_filha,
        {"partida_id": "1", "tipo_de_gol": ""},
        {"partida_id": "2", "tipo_de_gol": "Penalty"},
        {"partida_id": "3", "tipo_de_gol": "Gol Contra"},
    )

    tipos = {r["partida_id"]: r["tipo_de_gol"] for r in df.collect()}

    assert tipos == {1: None, 2: "Penalty", 3: "Gol Contra"}


def test_schema_silver_e_clube_canonico(bronze_filha, mapa_times):
    limpo = _gols(bronze_filha, {"clube": "Gremio FBPA", "rodata": "7"})

    df = limpeza.para_silver_filha(limpo, mapa_times, limpeza.COLUNAS_GOLS)

    assert df.dtypes == SCHEMA_SILVER
    linha = df.first()
    assert (linha["clube"], linha["rodada"], linha["atleta"]) == ("Grêmio", 7, "Pato")


def test_linhas_identicas_sao_removidas(bronze_filha, mapa_times):
    limpo = _gols(bronze_filha, {"minuto": "70"}, {"minuto": "70"}, {"minuto": "71"})

    df = limpeza.para_silver_filha(limpo, mapa_times, limpeza.COLUNAS_GOLS)

    assert sorted(r["minuto"] for r in df.collect()) == [70, 71]
