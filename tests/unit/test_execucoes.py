"""O registro de execução sem o Spark: o que vai para a linha e o que acontece quando gravar
falha. A gravação de verdade em Delta está em tests/integration/test_execucoes.py."""

import logging
from datetime import UTC, datetime

import pytest

from brasileirao import execucoes


@pytest.fixture
def gravadas():
    return []


def _registrar(gravadas, comando="pipeline gold"):
    return execucoes.registrar(comando, gravar=gravadas.append)


def test_sucesso_grava_linhas_e_avisos(gravadas, monkeypatch):
    monkeypatch.setenv("BRASILEIRAO_SPARK_ENV", "aws")
    with _registrar(gravadas):
        execucoes.contar("bronze/partidas", 10)
        execucoes.contar("bronze/partidas", 5)
        execucoes.contar("gold/campanhas", 3)
        logging.getLogger("brasileirao.teste").warning("2 partidas sem arena")
        logging.getLogger("brasileirao.teste").info("isto não é aviso")

    [execucao] = gravadas
    assert execucao.status == execucoes.SUCESSO
    assert execucao.comando == "pipeline gold"
    assert execucao.alvo == "aws"
    assert execucao.linhas == {"bronze/partidas": 15, "gold/campanhas": 3}
    assert execucao.avisos == ["brasileirao.teste: 2 partidas sem arena"]
    assert execucao.inicio <= execucao.fim <= datetime.now(UTC)
    # Fora do bloco, contar não vai para lugar nenhum.
    execucoes.contar("gold/campanhas", 99)
    assert execucao.linhas["gold/campanhas"] == 3


def test_falha_grava_e_repassa_a_mesma_excecao(gravadas):
    erro = RuntimeError("gold reprovado")
    with pytest.raises(RuntimeError) as capturada, _registrar(gravadas):
        raise erro

    assert capturada.value is erro
    [execucao] = gravadas
    assert execucao.status == execucoes.FALHA
    assert execucao.avisos == ["Erro: RuntimeError: gold reprovado"]


def test_systemexit_mantem_o_codigo(gravadas):
    with pytest.raises(SystemExit) as saida, _registrar(gravadas):
        raise SystemExit("Erro: silver ausente, rode make lakehouse")
    assert saida.value.code == "Erro: silver ausente, rode make lakehouse"
    assert gravadas[0].status == execucoes.FALHA
    assert gravadas[0].avisos == ["Erro: silver ausente, rode make lakehouse"]

    with pytest.raises(SystemExit) as saida, _registrar(gravadas):
        raise SystemExit(0)
    assert saida.value.code == 0
    assert gravadas[1].status == execucoes.SUCESSO


def _gravar_quebrado(_):
    raise OSError("disco cheio")


def test_falha_ao_gravar_nao_derruba_o_sucesso(caplog):
    with execucoes.registrar("pipeline gold", gravar=_gravar_quebrado):
        resultado = "terminou"

    assert resultado == "terminou"
    assert "não registrada" in caplog.text
    assert "OSError: disco cheio" in caplog.text


def test_falha_ao_gravar_nao_troca_o_erro_da_execucao(caplog):
    with (
        pytest.raises(SystemExit) as saida,
        execucoes.registrar("pipeline gold", gravar=_gravar_quebrado),
    ):
        raise SystemExit("Erro: gold reprovado")

    assert saida.value.code == "Erro: gold reprovado"
    assert "não registrada" in caplog.text


def test_avisos_tem_limite(gravadas):
    with _registrar(gravadas):
        for i in range(execucoes.MAX_AVISOS + 10):
            logging.getLogger("brasileirao.teste").warning("aviso %d %s", i, "x" * 2000)

    avisos = gravadas[0].avisos
    assert len(avisos) == execucoes.MAX_AVISOS + 1
    assert len(avisos[0]) == execucoes.MAX_TEXTO
    assert avisos[-1].startswith("... avisos demais")


def test_comando_de():
    assert execucoes.comando_de("simulacao", ["--destino", "data_sim", "gold", "5"]) == (
        "simulacao --destino data_sim gold 5"
    )


def test_formatar_sem_execucao(monkeypatch):
    monkeypatch.setenv("BRASILEIRAO_DATA_ROOT", "data_sim")
    assert execucoes.formatar([]) == "nenhuma execução registrada em data_sim/ops/execucoes"


def test_formatar():
    linha = {
        "id": "0123456789abcdef",
        "comando": "simulacao gold 5",
        "alvo": "local",
        "inicio": datetime(2026, 11, 12, 10, 0, 0),
        "fim": datetime(2026, 11, 12, 10, 0, 42),
        "status": "falha",
        "linhas": {"gold/a": 2, "gold/b": 3},
        "avisos": ["Erro: gold reprovado:\n- viradas: 8785"],
    }

    texto = execucoes.formatar([linha])

    assert "2026-11-12 10:00:00  falha      42s  local  01234567  simulacao gold 5" in texto
    assert "    linhas: 5 (gold/a 2, gold/b 3)" in texto
    assert "    aviso: Erro: gold reprovado:\n           - viradas: 8785" in texto
