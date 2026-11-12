"""Uma rodada da simulação de 2025: retrato → bronze/silver → gold → checagens → registro.

Disparo manual, com `rodada` (1 a 38) e `alvo` (`local`, o lake em data_sim, ou `aws`, o Spark
local com o lake em `s3a://brasileirao-lake-<conta>/data_sim`). Uma tarefa que falha deixa as
seguintes em `upstream_failed`. Como usar em docs/orchestration/README.md.
"""

from datetime import UTC, datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param, chain, task

from brasileirao import orquestracao

with DAG(
    dag_id=orquestracao.DAG_ID,
    description="Simulação: uma rodada de 2025 do retrato ao gold",
    doc_md=__doc__,
    # Manual: a rodada simulada não segue o calendário (ver o README).
    schedule=None,
    catchup=False,
    # Duas execuções ao mesmo tempo gravariam no mesmo lake.
    max_active_runs=1,
    # Pausada, a execução disparada fica na fila; sem agendamento, despausar não dispara nada.
    is_paused_upon_creation=False,
    params={
        "rodada": Param(
            type="integer",
            minimum=1,
            maximum=orquestracao.RODADAS,
            description=f"rodada de {orquestracao.TEMPORADA} a processar",
        ),
        "alvo": Param(
            "local",
            type="string",
            enum=list(orquestracao.ALVOS),
            description="local (data_sim) ou aws (lake no S3, só com a infra de pé)",
        ),
    },
    tags=["brasileirao", "simulacao"],
) as dag:
    etapas = [
        BashOperator(
            task_id=tarefa,
            bash_command=orquestracao.comando(etapa),
            env=orquestracao.AMBIENTE,
            append_env=True,
            cwd=orquestracao.PASTA_PROJETO,
        )
        for tarefa, etapa in orquestracao.ETAPAS
    ]

    @task(task_id=orquestracao.TAREFA_REGISTRO)
    def registra_execucao(params=None, dag_run=None) -> None:
        """Provisória: uma linha JSON no log. O OR3 troca pela tabela `ops/execucoes`."""
        print(
            orquestracao.registro_execucao(
                dag_run.run_id,
                params["rodada"],
                params["alvo"],
                dag_run.start_date,
                datetime.now(UTC),
            )
        )

    chain(*etapas, registra_execucao())
