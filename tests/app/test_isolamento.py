"""O app publicado roda sem Spark: não importa o pipeline nem depende dele."""

import ast
import re
import tomllib
from pathlib import Path

from brasileirao.aggregate import gold

RAIZ = Path(__file__).parent.parent.parent
APP = RAIZ / "app"
PROIBIDOS = {"brasileirao", "pyspark", "delta", "psycopg", "kagglehub", "py4j"}


def _pins_do_requirements() -> dict[str, str]:
    linhas = (APP / "requirements.txt").read_text().splitlines()
    # Tira o marcador de plataforma: `watchdog==6.0.0 ; sys_platform != 'darwin'`.
    return dict(
        linha.split(";")[0].strip().split("==")
        for linha in linhas
        if linha and not linha.startswith((" ", "#"))
    )


def _modulos_importados(arquivo: Path) -> set[str]:
    modulos = set()
    for no in ast.walk(ast.parse(arquivo.read_text())):
        if isinstance(no, ast.Import):
            modulos |= {a.name.split(".")[0] for a in no.names}
        elif isinstance(no, ast.ImportFrom) and no.module:
            modulos.add(no.module.split(".")[0])
    return modulos


def test_app_nao_importa_pipeline_nem_banco():
    arquivos = list(APP.rglob("*.py"))

    assert len(arquivos) == 10
    for arquivo in arquivos:
        assert not _modulos_importados(arquivo) & PROIBIDOS, arquivo


def test_requirements_nao_traz_o_pipeline():
    pins = _pins_do_requirements()

    assert {"streamlit", "altair", "pandas", "pyarrow"} <= set(pins)
    assert not set(pins) & {"pyspark", "delta-spark", "psycopg", "kagglehub", "py4j"}


def test_requirements_bate_com_o_lock_e_com_o_pyproject():
    # Trocou a versão no pyproject: rode `uv lock` e o comando no topo do requirements.txt.
    pins = _pins_do_requirements()
    lock = tomllib.loads((RAIZ / "uv.lock").read_text())
    versoes = {pacote["name"]: pacote["version"] for pacote in lock["package"]}
    grupo = tomllib.loads((RAIZ / "pyproject.toml").read_text())["dependency-groups"]["app"]

    for nome, versao in pins.items():
        assert versoes[re.sub(r"[-_.]+", "-", nome).lower()] == versao, nome
    for requisito in grupo:
        nome, versao = requisito.split("==")
        assert pins[nome] == versao


def test_dados_tem_um_parquet_por_tabela_do_gold():
    arquivos = {arquivo.stem for arquivo in (APP / "dados").glob("*.parquet")}

    assert arquivos == set(gold.TABELAS)
    assert len(arquivos) == 18
