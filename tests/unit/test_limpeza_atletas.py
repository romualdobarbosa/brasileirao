import pytest

from brasileirao.transform import limpeza
from brasileirao.transform.silver import carregar_mapa_atletas, carregar_mapa_times


@pytest.fixture
def mapa_atletas(spark):
    """Mapa pequeno: duas grafias do mesmo jogador e um homônimo separado pelo clube."""
    return spark.createDataFrame(
        [
            ("Gabriel Barbosa", None, "Gabigol"),
            ("Gabriel Barbosa Almeida", None, "Gabigol"),
            ("Raphael Cavalcante Veiga", None, "Raphael Veiga"),
            ("Fred", "Grêmio", "Fred (Grêmio)"),
            ("Mário Sérgio Santos Costa", None, "Marinho"),
            ("Mário Sérgio Santos Costa", "Santos", "Marinho do Santos"),
        ],
        "nome_fonte string, clube string, nome_popular string",
    )


def _gols(bronze_filha, *linhas):
    """Cada linha é (atleta, clube), com o clube já canônico, como depois de `normalizar_times`."""
    return limpeza.limpar_gols(
        bronze_filha(
            "gols",
            *[
                {"partida_id": str(i), "atleta": atleta, "clube": clube}
                for i, (atleta, clube) in enumerate(linhas, start=1)
            ],
        )
    )


def _atletas(df):
    return {r["partida_id"]: r["atleta"] for r in df.collect()}


def test_nome_mapeado_vira_o_popular(bronze_filha, mapa_atletas):
    df = limpeza.padronizar_atletas(
        _gols(bronze_filha, ("Raphael Cavalcante Veiga", "Palmeiras")), mapa_atletas
    )

    assert _atletas(df) == {1: "Raphael Veiga"}


def test_nome_sem_mapeamento_passa_direto(bronze_filha, mapa_atletas):
    df = limpeza.padronizar_atletas(
        _gols(bronze_filha, ("Hulk", "Atlético-MG"), (None, "Santos")), mapa_atletas
    )

    assert _atletas(df) == {1: "Hulk", 2: None}


def test_duas_grafias_viram_um_nome_so(bronze_filha, mapa_atletas):
    limpo = _gols(
        bronze_filha, ("Gabriel Barbosa", "Flamengo"), ("Gabriel Barbosa Almeida", "Flamengo")
    )

    df = limpeza.padronizar_atletas(limpo, mapa_atletas)

    assert _atletas(df) == {1: "Gabigol", 2: "Gabigol"}


def test_linha_com_clube_vale_so_para_aquele_clube(bronze_filha, mapa_atletas):
    limpo = _gols(bronze_filha, ("Fred", "Fluminense"), ("Fred", "Grêmio"), ("Fred", None))

    df = limpeza.padronizar_atletas(limpo, mapa_atletas)

    assert _atletas(df) == {1: "Fred", 2: "Fred (Grêmio)", 3: "Fred"}


def test_linha_com_clube_ganha_da_geral_sem_duplicar(bronze_filha, mapa_atletas):
    limpo = _gols(
        bronze_filha,
        ("Mário Sérgio Santos Costa", "Santos"),
        ("Mário Sérgio Santos Costa", "Vitória"),
    )

    df = limpeza.padronizar_atletas(limpo, mapa_atletas)

    assert df.count() == 2
    assert df.columns == limpo.columns
    assert _atletas(df) == {1: "Marinho do Santos", 2: "Marinho"}


def test_silver_casa_o_clube_ja_canonico(bronze_filha, mapa_times, mapa_atletas):
    limpo = limpeza.limpar_cartoes(
        bronze_filha(
            "cartoes",
            {"clube": "Gremio FBPA", "atleta": "Fred"},
            {"clube": "Santos", "atleta": "Gabriel Barbosa", "minuto": "10"},
        )
    )

    df = limpeza.para_silver_filha(limpo, mapa_times, limpeza.COLUNAS_CARTOES, mapa_atletas)

    assert df.columns == limpeza.COLUNAS_CARTOES
    assert {(r["clube"], r["atleta"]) for r in df.collect()} == {
        ("Grêmio", "Fred (Grêmio)"),
        ("Santos", "Gabigol"),
    }


def test_mapa_de_atletas_do_pacote(spark):
    mapa = carregar_mapa_atletas(spark).collect()

    chaves = [(r["nome_fonte"], r["clube"]) for r in mapa]
    assert len(chaves) == len(set(chaves))
    assert all(r["nome_popular"] and r["nome_popular"] != r["nome_fonte"] for r in mapa)
    canonicos = {r["nome_canonico"] for r in carregar_mapa_times(spark).collect()}
    assert {r["clube"] for r in mapa if r["clube"]} <= canonicos
    populares = {(r["nome_fonte"], r["clube"]): r["nome_popular"] for r in mapa}
    assert populares[("Gabriel Barbosa", None)] == "Gabigol"
    assert populares[("Gabriel Barbosa Almeida", None)] == "Gabigol"
    assert populares[("Fred", "Grêmio")] == "Fred (Grêmio)"
