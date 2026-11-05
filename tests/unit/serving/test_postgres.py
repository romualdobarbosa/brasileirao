import socket

import pytest

from brasileirao.serving import postgres


def _porta_fechada() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_postgres_fora_do_ar_da_erro_claro(monkeypatch):
    monkeypatch.setenv("BRASILEIRAO_PG_HOST", "127.0.0.1")
    monkeypatch.setenv("BRASILEIRAO_PG_PORT", str(_porta_fechada()))
    monkeypatch.setenv("BRASILEIRAO_PG_SENHA", "x")

    with pytest.raises(postgres.PostgresIndisponivelError, match="make serving-up"):
        postgres.conectar()
