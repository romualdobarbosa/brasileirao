"""O que a DAG `brasileirao_rodada` (orchestration/dags/) roda, sem importar o Airflow.

A DAG só monta as tarefas com o que está aqui. Assim a ordem, os comandos e o registro da
execução têm teste na suíte normal, que não instala o Airflow. O alerta de falha fica em
`brasileirao.alerta`, pelo mesmo motivo.

Cada tarefa do Spark é um processo `python -m brasileirao.simulacao <etapa> N` novo: a sessão
abre e fecha na tarefa, e a falha é o código de saída. A rodada e o alvo chegam pelo ambiente
(`RODADA`, `ALVO`), já validados pelos parâmetros da DAG, e nunca entram no texto do comando.
"""

import json
from datetime import datetime

DAG_ID = "brasileirao_rodada"
ALVOS = ("local", "aws")
TEMPORADA = 2025
RODADAS = 38
# Onde o projeto mora na imagem (orchestration/Dockerfile), com data/raw e data_sim montados.
PASTA_PROJETO = "/opt/brasileirao"
# (tarefa, subcomando de `brasileirao.simulacao`), na ordem em que rodam.
ETAPAS = (
    ("retrato", "retrato"),
    ("bronze_silver", "lakehouse"),
    ("gold", "gold"),
    ("checagens", "checar"),
)
# Resumo da execução da DAG no log. O registro de cada etapa é a `ops/execucoes`, gravada pela
# própria CLI, inclusive em falha.
TAREFA_REGISTRO = "registra_execucao"
AMBIENTE = {"RODADA": "{{ params.rodada }}", "ALVO": "{{ params.alvo }}"}

# O alvo aws é o mesmo `AWS_ENV` do Makefile: Spark local, lake no bucket do infra/main, numa
# pasta data_sim própria, e credencial só do perfil (a exportada no ambiente sai antes).
_COMANDO = """set -euo pipefail
case "$ALVO" in
  local)
    destino=data_sim
    ;;
  aws)
    unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_CREDENTIAL_EXPIRATION
    export AWS_PROFILE="${{AWS_PROFILE:-brasileirao}}" AWS_REGION=us-east-1
    export BRASILEIRAO_SPARK_ENV=aws
    conta=$(aws sts get-caller-identity --query Account --output text)
    destino="s3a://brasileirao-lake-$conta/data_sim"
    ;;
  *)
    echo "alvo desconhecido: $ALVO" >&2
    exit 2
    ;;
esac
exec python -m brasileirao.simulacao --destino "$destino" --temporada {temporada} {etapa} "$RODADA"
"""


def comando(etapa: str) -> str:
    """O script bash da tarefa que roda a `etapa` da simulação."""
    subcomandos = {sub for _, sub in ETAPAS}
    if etapa not in subcomandos:
        raise ValueError(f"etapa desconhecida: {etapa!r}. Use uma de: {sorted(subcomandos)}")
    return _COMANDO.format(temporada=TEMPORADA, etapa=etapa)


def registro_execucao(
    run_id: str, rodada: int, alvo: str, inicio: datetime | None, fim: datetime
) -> str:
    """Uma linha JSON com a execução, para o log da tarefa de registro."""
    return json.dumps(
        {
            "evento": "execucao_brasileirao",
            "dag_id": DAG_ID,
            "run_id": run_id,
            "temporada": TEMPORADA,
            "rodada": rodada,
            "alvo": alvo,
            "status": "sucesso",
            "inicio": inicio.isoformat() if inicio else None,
            "fim": fim.isoformat(),
            "segundos": round((fim - inicio).total_seconds(), 1) if inicio else None,
            "tarefas": [tarefa for tarefa, _ in ETAPAS],
        },
        ensure_ascii=False,
    )
