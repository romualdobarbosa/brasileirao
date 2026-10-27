from brasileirao.transform import limpeza

SCHEMA_SILVER = [
    ("partida_id", "int"),
    ("temporada", "int"),
    ("rodada", "int"),
    ("clube", "string"),
    ("cartao", "string"),
    ("atleta", "string"),
    ("num_camisa", "int"),
    ("posicao", "string"),
    ("minuto", "int"),
    ("acrescimo", "int"),
]


def _cartoes(bronze_filha, *linhas):
    return limpeza.limpar_cartoes(bronze_filha("cartoes", *linhas))


def test_tipa_camisa_e_minuto(bronze_filha):
    linha = _cartoes(bronze_filha, {"num_camisa": "28", "minuto": "90+3"}).first()

    assert (linha["num_camisa"], linha["minuto"], linha["acrescimo"]) == (28, 90, 3)


def test_camisa_vazia_vira_nulo(bronze_filha):
    assert _cartoes(bronze_filha, {"num_camisa": ""}).first()["num_camisa"] is None


def test_zagueira_vira_zagueiro_e_vazia_vira_nulo(bronze_filha):
    df = _cartoes(
        bronze_filha,
        {"partida_id": "1", "posicao": "Zagueira"},
        {"partida_id": "2", "posicao": ""},
        {"partida_id": "3", "posicao": "Meio-campo"},
    )

    posicoes = {r["partida_id"]: r["posicao"] for r in df.collect()}

    assert posicoes == {1: "Zagueiro", 2: None, 3: "Meio-campo"}


def test_schema_silver(bronze_filha, mapa_times):
    df = limpeza.para_silver_filha(
        _cartoes(bronze_filha, {"clube": "Gremio"}), mapa_times, limpeza.COLUNAS_CARTOES
    )

    assert df.dtypes == SCHEMA_SILVER
    assert df.first()["clube"] == "Grêmio"
