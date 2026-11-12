"""O que a DAG `brasileirao_rodada` roda, sem o Airflow. A DAG em si tem os testes dela em
`orchestration/tests` (`make test-dag`)."""

import json
import re
import subprocess
import tomllib
from datetime import UTC, datetime
from pathlib import Path

import pytest

from brasileirao import orquestracao

RAIZ = Path(__file__).parent.parent.parent
# Imprime os argumentos e o ambiente que importa, no lugar do `python` e do `aws` de verdade.
FALSO_PYTHON = """#!/bin/sh
echo "args=$*"
echo "spark_env=${BRASILEIRAO_SPARK_ENV:-}"
echo "perfil=${AWS_PROFILE:-}"
echo "chave=${AWS_ACCESS_KEY_ID:-}"
"""
FALSO_AWS = (
    '#!/bin/sh\n[ "$*" = "sts get-caller-identity --query Account --output text" ] && echo 123\n'
)


@pytest.fixture
def bin_falso(tmp_path):
    for nome, texto in (("python", FALSO_PYTHON), ("aws", FALSO_AWS)):
        arquivo = tmp_path / nome
        arquivo.write_text(texto)
        arquivo.chmod(0o755)
    return tmp_path


def _rodar(etapa: str, alvo: str, bin_falso: Path, **extra) -> subprocess.CompletedProcess:
    ambiente = {
        "PATH": f"{bin_falso}:/usr/bin:/bin",
        "RODADA": "7",
        "ALVO": alvo,
        "AWS_ACCESS_KEY_ID": "velha",
        **extra,
    }
    return subprocess.run(
        ["bash", "-c", orquestracao.comando(etapa)],
        env=ambiente,
        capture_output=True,
        text=True,
        check=False,
    )


def test_comando_local(bin_falso):
    saida = _rodar("retrato", "local", bin_falso)

    assert saida.returncode == 0, saida.stderr
    assert "args=-m brasileirao.simulacao --destino data_sim --temporada 2025 retrato 7" in (
        saida.stdout
    )
    assert "spark_env=\n" in saida.stdout


def test_comando_aws_usa_o_perfil_e_a_pasta_da_simulacao(bin_falso):
    saida = _rodar("gold", "aws", bin_falso)

    assert saida.returncode == 0, saida.stderr
    assert (
        "args=-m brasileirao.simulacao --destino s3a://brasileirao-lake-123/data_sim "
        "--temporada 2025 gold 7"
    ) in saida.stdout
    # Credencial exportada sai, para não passar na frente do perfil.
    assert "spark_env=aws\nperfil=brasileirao\nchave=\n" in saida.stdout
    assert "perfil=outro" in _rodar("gold", "aws", bin_falso, AWS_PROFILE="outro").stdout


def test_comando_aws_sem_credencial_nao_roda_o_spark(bin_falso):
    (bin_falso / "aws").write_text("#!/bin/sh\necho sem sessao >&2\nexit 255\n")

    saida = _rodar("lakehouse", "aws", bin_falso)

    assert saida.returncode != 0
    assert "args=" not in saida.stdout


def test_comando_alvo_desconhecido(bin_falso):
    saida = _rodar("checar", "gcp", bin_falso)

    assert saida.returncode == 2
    assert "alvo desconhecido: gcp" in saida.stderr


def test_etapas_e_comando_desconhecido():
    assert [t for t, _ in orquestracao.ETAPAS] == ["retrato", "bronze_silver", "gold", "checagens"]
    with pytest.raises(ValueError, match="etapa desconhecida"):
        orquestracao.comando("exportar")


def test_registro_execucao():
    inicio = datetime(2026, 11, 11, 22, 0, 0, tzinfo=UTC)
    fim = datetime(2026, 11, 11, 22, 1, 30, tzinfo=UTC)

    registro = json.loads(orquestracao.registro_execucao("manual__x", 4, "local", inicio, fim))

    assert registro["evento"] == "execucao_brasileirao"
    assert (registro["rodada"], registro["alvo"], registro["status"]) == (4, "local", "sucesso")
    assert registro["segundos"] == 90.0
    assert registro["tarefas"] == ["retrato", "bronze_silver", "gold", "checagens"]


def test_versoes_da_imagem_batem_com_o_projeto():
    dockerfile = (RAIZ / "orchestration" / "Dockerfile").read_text()
    spark = tomllib.loads((RAIZ / "pyproject.toml").read_text())["dependency-groups"]["spark"]
    pins = dict(p.split("==") for p in spark)
    args = dict(re.findall(r"^ARG (\w+)=(\S+)$", dockerfile, re.MULTILINE))
    assert args["PYSPARK_VERSION"] == pins["pyspark"]
    assert args["DELTA_SPARK_VERSION"] == pins["delta-spark"]

    airflow = re.search(r"^FROM apache/airflow:(\S+)-python3\.12$", dockerfile, re.MULTILINE)[1]
    makefile = (RAIZ / "Makefile").read_text()
    assert re.search(r"^AIRFLOW_VERSION = (\S+)$", makefile, re.MULTILINE)[1] == airflow
    compose = (RAIZ / "orchestration" / "docker-compose.yml").read_text()
    assert f"image: brasileirao-airflow:{airflow}\n" in compose
