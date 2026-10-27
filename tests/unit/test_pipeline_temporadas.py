import pytest

from brasileirao import pipeline

FONTE_V18 = {2003: 552, 2004: 552, 2005: 462, 2016: 379, 2019: 380, 2020: 380, 2025: 380}


def test_sem_temporada_em_andamento_quando_a_ultima_esta_completa():
    assert pipeline.temporada_em_andamento(FONTE_V18) is None
    assert pipeline.temporadas_encerradas(FONTE_V18) == sorted(FONTE_V18)


def test_ultima_temporada_incompleta_esta_em_andamento():
    contagens = {**FONTE_V18, 2026: 120}

    assert pipeline.temporada_em_andamento(contagens) == 2026
    assert 2026 not in pipeline.temporadas_encerradas(contagens)


def test_so_a_maior_temporada_pode_estar_em_andamento():
    contagens = {2015: 200, 2016: 379}

    assert pipeline.temporada_em_andamento(contagens) is None


def test_fonte_vazia():
    assert pipeline.temporada_em_andamento({}) is None


def test_validar_temporada_em_andamento():
    with pytest.raises(pipeline.TemporadaInvalidaError, match="2026 está em andamento"):
        pipeline.validar_temporada(2026, {2025: 380, 2026: 120})


def test_validar_temporada_inexistente():
    with pytest.raises(pipeline.TemporadaInvalidaError, match="1999 não existe.*2003–2025"):
        pipeline.validar_temporada(1999, FONTE_V18)


def test_validar_temporada_encerrada():
    pipeline.validar_temporada(2019, FONTE_V18)
