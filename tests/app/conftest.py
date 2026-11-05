from pathlib import Path

import pytest

APP = Path(__file__).parent.parent.parent / "app"


@pytest.fixture(autouse=True)
def caminho_do_app(monkeypatch):
    """`streamlit run` põe a pasta do app no sys.path; o AppTest não."""
    monkeypatch.syspath_prepend(str(APP))
