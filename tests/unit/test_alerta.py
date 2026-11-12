"""O alerta de falha por e-mail, com um SMTP falso no lugar do `smtplib`. Nenhum e-mail sai."""

import logging
import smtplib
from types import SimpleNamespace

import pytest

from brasileirao import alerta

AMBIENTE = {
    "ALERTA_SMTP_HOST": "smtp.exemplo.com",
    "ALERTA_SMTP_USUARIO": "robo@exemplo.com",
    "ALERTA_SMTP_SENHA": "senha-de-teste",
    "ALERTA_EMAIL_PARA": "eu@exemplo.com",
}
FALHA = alerta.Falha(
    dag_id="brasileirao_rodada",
    run_id="rodada-05-falha",
    tarefa="gold",
    rodada=5,
    alvo="local",
    erro="Bash command failed. The command returned a non-zero exit code 1.",
    log_url="http://localhost:8080/dags/brasileirao_rodada/runs/rodada-05-falha/tasks/gold",
)


class SmtpFalso:
    """Guarda o que o alerta faria com o servidor."""

    instancias: list["SmtpFalso"] = []
    falhar_em: str | None = None

    def __init__(self, host, porta, timeout=None, context=None):
        self.host, self.porta, self.ssl = host, porta, context is not None
        self.passos: list = []
        SmtpFalso.instancias.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.passos.append("quit")

    def _passo(self, nome, *args):
        if SmtpFalso.falhar_em == nome:
            raise smtplib.SMTPAuthenticationError(535, b"senha errada")
        self.passos.append((nome, *args))

    def starttls(self, context=None):
        self._passo("starttls")

    def login(self, usuario, senha):
        self._passo("login", usuario, senha)

    def send_message(self, mensagem):
        self._passo("send_message", mensagem)


@pytest.fixture
def smtp(monkeypatch):
    SmtpFalso.instancias = []
    SmtpFalso.falhar_em = None
    monkeypatch.setattr(alerta.smtplib, "SMTP", SmtpFalso)
    monkeypatch.setattr(alerta.smtplib, "SMTP_SSL", SmtpFalso)
    return SmtpFalso


def test_sem_configuracao_fica_desligado(smtp, caplog):
    caplog.set_level(logging.INFO, logger="brasileirao.alerta")
    falta_senha = {k: v for k, v in AMBIENTE.items() if k != "ALERTA_SMTP_SENHA"}

    assert alerta.alertar(FALHA, {}) is False
    assert alerta.alertar(FALHA, falta_senha) is False

    assert smtp.instancias == []
    assert "alerta desligado" in caplog.text
    assert "ALERTA_SMTP_SENHA" in caplog.text


def test_envia_com_starttls_na_587(smtp):
    assert alerta.alertar(FALHA, AMBIENTE) is True

    [conexao] = smtp.instancias
    assert (conexao.host, conexao.porta, conexao.ssl) == ("smtp.exemplo.com", 587, False)
    nomes = [p if isinstance(p, str) else p[0] for p in conexao.passos]
    assert nomes == ["starttls", "login", "send_message", "quit"]
    assert conexao.passos[1] == ("login", "robo@exemplo.com", "senha-de-teste")
    mensagem = conexao.passos[2][1]
    assert mensagem["To"] == "eu@exemplo.com"
    assert mensagem["From"] == "robo@exemplo.com"
    assert "gold" in mensagem["Subject"] and "rodada 5" in mensagem["Subject"]
    corpo = mensagem.get_content()
    for trecho in (
        "dag_id: brasileirao_rodada",
        "run_id: rodada-05-falha",
        "tarefa: gold",
        "rodada: 5",
        "alvo: local",
        "non-zero exit code 1",
        "log: http://localhost:8080/dags/brasileirao_rodada/runs/rodada-05-falha/tasks/gold",
    ):
        assert trecho in corpo


def test_envia_com_ssl_na_465(smtp):
    ambiente = {**AMBIENTE, "ALERTA_SMTP_PORTA": "465", "ALERTA_EMAIL_DE": "alerta@exemplo.com"}

    assert alerta.alertar(FALHA, ambiente) is True

    [conexao] = smtp.instancias
    assert (conexao.porta, conexao.ssl) == (465, True)
    assert "starttls" not in conexao.passos
    assert conexao.passos[1][1]["From"] == "alerta@exemplo.com"


def test_envio_que_falha_nao_propaga(smtp, caplog):
    smtp.falhar_em = "login"

    assert alerta.alertar(FALHA, AMBIENTE) is False

    assert "alerta não enviado" in caplog.text
    assert "SMTPAuthenticationError" in caplog.text
    assert "senha-de-teste" not in caplog.text


def test_servidor_fora_do_ar_nao_propaga(monkeypatch, caplog):
    def recusa(*_, **__):
        raise ConnectionRefusedError("conexão recusada")

    monkeypatch.setattr(alerta.smtplib, "SMTP", recusa)

    assert alerta.alertar(FALHA, AMBIENTE) is False
    assert "ConnectionRefusedError" in caplog.text


def test_porta_invalida_desliga(smtp):
    assert alerta.config_do_ambiente({**AMBIENTE, "ALERTA_SMTP_PORTA": "abc"}) is None
    assert alerta.alertar(FALHA, {**AMBIENTE, "ALERTA_SMTP_PORTA": "abc"}) is False
    assert smtp.instancias == []


def test_config_nao_mostra_a_senha():
    assert "senha-de-teste" not in repr(alerta.config_do_ambiente(AMBIENTE))


def _contexto():
    ti = SimpleNamespace(
        dag_id="brasileirao_rodada",
        task_id="gold",
        run_id="rodada-05-falha",
        log_url="http://localhost:8080/x",
    )
    return {
        "ti": ti,
        "dag_run": SimpleNamespace(run_id="rodada-05-falha", dag_id="brasileirao_rodada"),
        "params": {"rodada": 5, "alvo": "local"},
        "exception": RuntimeError("exit code 1"),
    }


def test_falha_do_contexto():
    assert alerta.falha_do_contexto(_contexto()) == alerta.Falha(
        "brasileirao_rodada", "rodada-05-falha", "gold", 5, "local", "exit code 1",
        "http://localhost:8080/x",
    )  # fmt: skip


def test_callback_sem_configuracao_so_loga(smtp, monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="brasileirao.alerta")
    for variavel in (*alerta.OBRIGATORIAS, "ALERTA_SMTP_PORTA"):
        monkeypatch.delenv(variavel, raising=False)

    assert alerta.ao_falhar(_contexto()) is None

    assert smtp.instancias == []
    assert "alerta desligado: falha de gold em rodada-05-falha" in caplog.text


def test_callback_envia_com_o_ambiente(smtp, monkeypatch):
    for variavel, valor in AMBIENTE.items():
        monkeypatch.setenv(variavel, valor)

    alerta.ao_falhar(_contexto())

    assert len(smtp.instancias) == 1


def test_callback_nunca_levanta(monkeypatch, caplog):
    def quebra(_):
        raise KeyError("contexto estranho")

    monkeypatch.setattr(alerta, "falha_do_contexto", quebra)

    alerta.ao_falhar({})

    assert "erro inesperado no callback" in caplog.text
