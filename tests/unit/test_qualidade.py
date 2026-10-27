import pytest
from pyspark.sql import functions as F

from brasileirao.transform import limpeza, qualidade


def _limpo(bronze_partidas, *linhas):
    return limpeza.deduplicar_partidas(limpeza.limpar_partidas(bronze_partidas(*linhas)))


def _checar(df, mapa):
    qualidade.checar_falhas_partidas(df, mapa, limpeza.COLUNAS_TIME_PARTIDAS, 2019)


def test_partidas_validas_passam(bronze_partidas, mapa_times):
    _checar(_limpo(bronze_partidas, {"ID": "1"}, {"ID": "2", "vencedor": "-"}), mapa_times)


def test_times_sem_mapeamento_lista_os_nomes(bronze_partidas, mapa_times):
    df = _limpo(
        bronze_partidas,
        {"ID": "1", "mandante": "Time B", "vencedor": "Time B"},
        {"ID": "2", "visitante": "Time A", "vencedor": "-"},
    )

    assert qualidade.times_sem_mapeamento(df, mapa_times, limpeza.COLUNAS_TIME_PARTIDAS) == [
        "Time A",
        "Time B",
    ]
    with pytest.raises(qualidade.QualidadeError, match="times sem mapeamento.*Time A, Time B"):
        _checar(df, mapa_times)


def test_partida_id_nulo_falha(bronze_partidas, mapa_times):
    df = _limpo(bronze_partidas, {"ID": ""}, {"ID": "abc"}, {"ID": "1"})

    with pytest.raises(qualidade.QualidadeError, match="2 partida\\(s\\) com partida_id nulo"):
        _checar(df, mapa_times)


def test_partida_id_duplicado(bronze_partidas):
    df = limpeza.limpar_partidas(bronze_partidas({"ID": "5"}, {"ID": "5"}, {"ID": "6"}))

    assert qualidade.partida_ids_invalidos(df) == (0, [5])


def test_duplicado_falha(bronze_partidas, mapa_times):
    df = limpeza.limpar_partidas(bronze_partidas({"ID": "5"}, {"ID": "5"}))

    with pytest.raises(qualidade.QualidadeError, match="partida_id duplicado: 5"):
        _checar(df, mapa_times)


@pytest.mark.parametrize("placar", ["-1", "x", ""])
def test_placar_invalido_falha(bronze_partidas, mapa_times, placar):
    df = _limpo(bronze_partidas, {"ID": "1"}, {"ID": "9", "visitante_Placar": placar})

    with pytest.raises(qualidade.QualidadeError, match="placar.*partidas: 9$") as erro:
        _checar(df, mapa_times)
    assert len(erro.value.problemas) == 1


def test_falha_junta_todos_os_problemas(bronze_partidas, mapa_times):
    df = _limpo(bronze_partidas, {"ID": "", "mandante": "Time X", "mandante_Placar": "-2"})

    with pytest.raises(qualidade.QualidadeError) as erro:
        _checar(df, mapa_times)

    assert len(erro.value.problemas) == 3
    assert "temporada 2019" in str(erro.value)


def test_aviso_de_contagem_da_temporada(bronze_partidas, mapa_times):
    silver = limpeza.para_silver_partidas(_limpo(bronze_partidas, {"ID": "1"}), mapa_times)

    avisos = qualidade.avisos_partidas(silver, 2016)

    assert "1 partidas na temporada, esperado 379" in avisos


def test_aviso_de_nulos_por_coluna(bronze_partidas, mapa_times):
    df = _limpo(
        bronze_partidas,
        {"ID": "1", "tecnico_mandante": "", "arrecadacao": "100"},
        {"ID": "2", "tecnico_mandante": "", "hora": "", "arrecadacao": "200"},
    )
    silver = limpeza.para_silver_partidas(df, mapa_times)

    avisos = qualidade.avisos_partidas(silver, 2019)

    assert "nulos em colunas opcionais: hora=1, tecnico_mandante=2" in avisos


def test_sem_avisos_quando_tudo_bate(bronze_partidas, mapa_times):
    linhas = [{"ID": str(i), "arrecadacao": "1"} for i in range(380)]
    silver = limpeza.para_silver_partidas(_limpo(bronze_partidas, *linhas), mapa_times)

    assert qualidade.avisos_partidas(silver, 2019) == []


def test_sem_aviso_de_nulos_nos_buracos_conhecidos_de_partidas(bronze_partidas, mapa_times):
    sem_tecnico = {"tecnico_mandante": "", "formacao_visitante": "", "arrecadacao": None}
    df = _limpo(bronze_partidas, {"ID": "1", **sem_tecnico}, {"ID": "2", **sem_tecnico})
    silver = limpeza.para_silver_partidas(df, mapa_times)

    assert not any("nulos" in aviso for aviso in qualidade.avisos_partidas(silver, 2010))
    assert "nulos em colunas opcionais: tecnico_mandante=2" in (
        qualidade.avisos_partidas(silver, 2014)
    )
    assert "nulos em colunas opcionais: tecnico_mandante=2, formacao_visitante=2" in (
        qualidade.avisos_partidas(silver, 2015)
    )
    assert "nulos em colunas opcionais: arrecadacao=2" in qualidade.avisos_partidas(
        silver.withColumn("tecnico_mandante", F.lit("X")).withColumn(
            "formacao_visitante", F.lit("4-4-2")
        ),
        2025,
    )


@pytest.mark.parametrize(
    ("tabela", "temporada", "buraco"),
    [
        ("gols", 2013, True),
        ("gols", 2014, False),
        ("gols", 2025, False),
        ("cartoes", 2013, True),
        ("cartoes", 2024, False),
        ("cartoes", 2025, True),
        ("estatisticas", 2003, True),
        ("estatisticas", 2023, False),
        ("estatisticas", 2024, True),
        ("estatisticas", 2025, False),
    ],
)
def test_buracos_conhecidos_das_filhas(tabela, temporada, buraco):
    assert (qualidade.TABELA_INTEIRA in qualidade.buracos_conhecidos(tabela, temporada)) is buraco


@pytest.mark.parametrize(
    ("tabela", "temporada", "coluna", "buraco"),
    [
        ("partidas", 2013, "tecnico_mandante", True),
        ("partidas", 2014, "tecnico_mandante", False),
        ("partidas", 2014, "formacao_mandante", True),
        ("partidas", 2014, "formacao_visitante", True),
        ("partidas", 2015, "formacao_mandante", False),
        ("cartoes", 2014, "num_camisa", True),
        ("cartoes", 2015, "num_camisa", False),
        ("cartoes", 2014, "posicao", True),
        ("cartoes", 2024, "posicao", True),
        ("cartoes", 2019, "atleta", False),
        ("estatisticas", 2014, "precisao_passes", True),
        ("estatisticas", 2017, "precisao_passes", True),
        ("estatisticas", 2018, "precisao_passes", False),
        ("estatisticas", 2015, "posse_de_bola", False),
    ],
)
def test_buracos_conhecidos_por_coluna(tabela, temporada, coluna, buraco):
    assert (coluna in qualidade.buracos_conhecidos(tabela, temporada)) is buraco


def _silver_filha(bronze_filha, mapa_times, tabela, *linhas):
    limpar, colunas = {
        "gols": (limpeza.limpar_gols, limpeza.COLUNAS_GOLS),
        "cartoes": (limpeza.limpar_cartoes, limpeza.COLUNAS_CARTOES),
        "estatisticas": (limpeza.limpar_estatisticas, limpeza.COLUNAS_ESTATISTICAS),
    }[tabela]
    return limpeza.para_silver_filha(limpar(bronze_filha(tabela, *linhas)), mapa_times, colunas)


def test_aviso_de_nulos_na_filha(bronze_filha, mapa_times):
    df = _silver_filha(
        bronze_filha,
        mapa_times,
        "cartoes",
        {"partida_id": "1", "posicao": ""},
        {"partida_id": "2", "posicao": "", "num_camisa": ""},
    )

    assert qualidade.avisos_filha(df, "cartoes", 2019) == [
        "nulos em colunas opcionais: num_camisa=1"
    ]
    assert qualidade.avisos_filha(df, "cartoes", 2014) == []


def test_aviso_de_nulos_fora_dos_buracos_de_estatisticas(bronze_filha, mapa_times):
    df = _silver_filha(
        bronze_filha, mapa_times, "estatisticas", {"posse_de_bola": "", "precisao_passes": ""}
    )

    assert qualidade.avisos_filha(df, "estatisticas", 2016) == [
        "nulos em colunas opcionais: posse_de_bola=1"
    ]
    assert qualidade.avisos_filha(df, "estatisticas", 2018) == [
        "nulos em colunas opcionais: posse_de_bola=1, precisao_passes=1"
    ]


def test_aviso_de_temporada_vazia_so_fora_dos_buracos(bronze_filha, mapa_times):
    vazio = _silver_filha(bronze_filha, mapa_times, "cartoes", {}).limit(0)

    assert qualidade.avisos_filha(vazio, "cartoes", 2019) == ["nenhuma linha na temporada"]
    assert qualidade.avisos_filha(vazio, "cartoes", 2025) == []


def test_filha_sem_problemas_nao_avisa(bronze_filha, mapa_times):
    df = _silver_filha(bronze_filha, mapa_times, "gols", {"tipo_de_gol": "", "minuto": "45+1"})

    assert qualidade.avisos_filha(df, "gols", 2019) == []


def test_clube_sem_mapeamento_falha_na_filha(bronze_filha, mapa_times):
    limpo = limpeza.limpar_gols(bronze_filha("gols", {"clube": "Time Z"}, {"clube": "Santos"}))

    with pytest.raises(qualidade.QualidadeError, match="silver/gols.*\n.*Time Z$"):
        qualidade.checar_falhas_filha(limpo, mapa_times, "gols", 2019)


def test_cartao_fora_de_amarelo_e_vermelho_falha(bronze_filha, mapa_times):
    limpo = limpeza.limpar_cartoes(
        bronze_filha(
            "cartoes",
            {"cartao": "Amarelo"},
            {"cartao": "Vermelho"},
            {"cartao": "Azul"},
            {"cartao": ""},
        )
    )

    assert qualidade.cartoes_invalidos(limpo) == ["Azul", "None"]
    with pytest.raises(qualidade.QualidadeError, match="cartao fora de Amarelo/Vermelho: Azul"):
        qualidade.checar_falhas_filha(limpo, mapa_times, "cartoes", 2019)


def test_cartoes_validos_passam(bronze_filha, mapa_times):
    limpo = limpeza.limpar_cartoes(bronze_filha("cartoes", {"cartao": "Vermelho"}, {}))

    qualidade.checar_falhas_filha(limpo, mapa_times, "cartoes", 2019)


def test_estatistica_duplicada_por_partida_e_clube_falha(bronze_filha, mapa_times):
    df = _silver_filha(
        bronze_filha,
        mapa_times,
        "estatisticas",
        {"partida_id": "1", "clube": "Gremio", "chutes": "10"},
        {"partida_id": "1", "clube": "Gremio FBPA", "chutes": "11"},
        {"partida_id": "1", "clube": "Santos"},
    )

    with pytest.raises(
        qualidade.QualidadeError, match=r"\(partida_id, clube\) duplicado: \(1, Grêmio\)$"
    ):
        qualidade.checar_unicidade_estatisticas(df, 2019)


def test_estatistica_identica_nao_e_duplicada(bronze_filha, mapa_times):
    df = _silver_filha(
        bronze_filha, mapa_times, "estatisticas", {"partida_id": "1"}, {"partida_id": "1"}
    )

    qualidade.checar_unicidade_estatisticas(df, 2019)


def test_partida_ids_orfaos(spark):
    filha = spark.createDataFrame([("1",), ("1",), ("7",), ("9",)], "partida_id string")
    partidas = spark.createDataFrame([("1",), ("2",)], "partida_id string")

    assert qualidade.partida_ids_orfaos(filha, partidas) == (2, ["7", "9"])
