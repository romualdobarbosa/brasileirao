import pytest

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
