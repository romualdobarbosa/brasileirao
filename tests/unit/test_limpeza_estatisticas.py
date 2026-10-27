import pytest

from brasileirao.transform import limpeza

SCHEMA_SILVER = [
    ("partida_id", "int"),
    ("temporada", "int"),
    ("rodada", "int"),
    ("clube", "string"),
    ("chutes", "int"),
    ("chutes_no_alvo", "int"),
    ("posse_de_bola", "double"),
    ("passes", "int"),
    ("precisao_passes", "double"),
    ("faltas", "int"),
    ("cartao_amarelo", "int"),
    ("cartao_vermelho", "int"),
    ("impedimentos", "int"),
    ("escanteios", "int"),
]
ZERADA = {"chutes": "0", "chutes_no_alvo": "0", "passes": "0", "posse_de_bola": ""}


def _estatisticas(bronze_filha, *linhas):
    return limpeza.limpar_estatisticas(bronze_filha("estatisticas", *linhas))


def test_schema_silver(bronze_filha, mapa_times):
    df = limpeza.para_silver_filha(
        _estatisticas(bronze_filha, {}), mapa_times, limpeza.COLUNAS_ESTATISTICAS
    )

    assert df.dtypes == SCHEMA_SILVER


def test_tipa_inteiros_e_percentuais(bronze_filha):
    linha = _estatisticas(bronze_filha, {"posse_de_bola": "45%", "precisao_passes": "8%"}).first()

    assert (linha["chutes"], linha["passes"], linha["escanteios"]) == (15, 480, 7)
    assert linha["posse_de_bola"] == pytest.approx(0.45)
    assert linha["precisao_passes"] == pytest.approx(0.08)


@pytest.mark.parametrize("valor", ["None", "", " "])
def test_percentual_none_ou_vazio_vira_nulo(bronze_filha, valor):
    linha = _estatisticas(bronze_filha, {"posse_de_bola": valor, "precisao_passes": valor}).first()

    assert linha["posse_de_bola"] is None
    assert linha["precisao_passes"] is None


def test_descarta_linha_sem_dado_real(bronze_filha):
    df = _estatisticas(
        bronze_filha,
        {"partida_id": "1", **ZERADA},
        {"partida_id": "2", **ZERADA, "posse_de_bola": "None"},
        {"partida_id": "3", **ZERADA, "posse_de_bola": "0%"},
        {"partida_id": "4", **ZERADA, "chutes": "", "passes": ""},
        {"partida_id": "5"},
        {"partida_id": "6", "posse_de_bola": "None"},
        {"partida_id": "7", **ZERADA, "passes": "12"},
    )

    mantidas = limpeza.descartar_estatisticas_sem_dado(df)

    assert sorted(r["partida_id"] for r in mantidas.collect()) == [5, 6, 7]
