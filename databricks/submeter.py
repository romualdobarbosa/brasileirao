"""Sobe os scripts de `databricks/` ao workspace e submete um run único, serverless.

    uv run -q --no-project --with databricks-sdk python -I databricks/submeter.py sondagem
    uv run -q --no-project --with databricks-sdk python -I databricks/submeter.py pipeline
    uv run -q --no-project --with databricks-sdk python -I databricks/submeter.py wheel

A credencial vem do perfil DEFAULT de ~/.databrickscfg (o SDK lê sozinho); nada dela passa
por aqui. `jobs.submit` cria um run avulso: não fica job salvo nem agendamento.
`wheel` sobe o `dist/*.whl` de `make wheel` por cima do que está no Volume.
"""

import sys
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.workspace import ImportFormat

VOLUME = "/Volumes/workspace/default/brasileirao"
WHEEL = f"{VOLUME}/wheels/brasileirao-0.1.0-py3-none-any.whl"
# Versão do ambiente serverless: a 4 traz Python 3.12, que o pacote exige.
VERSAO_AMBIENTE = "4"
AQUI = Path(__file__).resolve().parent
SCRIPTS = ("sondagem.py", "executar.py", "foto.py")


def subir_scripts(w: WorkspaceClient) -> str:
    pasta = f"/Users/{w.current_user.me().user_name}/brasileirao"
    w.workspace.mkdirs(pasta)
    for nome in SCRIPTS:
        # Sem o cabeçalho de notebook, o .py entra como arquivo do workspace, que é o que a
        # spark_python_task executa.
        w.workspace.upload(
            f"{pasta}/{nome}", (AQUI / nome).read_bytes(), format=ImportFormat.AUTO, overwrite=True
        )
    return pasta


def task(chave: str, script: str, parametros: list[str], depois: str | None = None):
    return jobs.SubmitTask(
        task_key=chave,
        environment_key="brasileirao",
        spark_python_task=jobs.SparkPythonTask(
            python_file=script, parameters=parametros, source=jobs.Source.WORKSPACE
        ),
        depends_on=[jobs.TaskDependency(task_key=depois)] if depois else None,
    )


def submeter(w: WorkspaceClient, nome: str, tarefas: list) -> None:
    ambiente = jobs.JobEnvironment(
        environment_key="brasileirao",
        spec=compute.Environment(environment_version=VERSAO_AMBIENTE, dependencies=[WHEEL]),
    )
    espera = w.jobs.submit(run_name=nome, tasks=tarefas, environments=[ambiente])
    run_id = espera.run_id
    run = w.jobs.get_run(run_id)
    print(f"run_id={run_id}\nlink={run.run_page_url}", flush=True)
    inicio = time.monotonic()
    while True:
        run = w.jobs.get_run(run_id)
        if run.state.life_cycle_state in (
            jobs.RunLifeCycleState.TERMINATED,
            jobs.RunLifeCycleState.INTERNAL_ERROR,
            jobs.RunLifeCycleState.SKIPPED,
        ):
            break
        time.sleep(20)
    print(f"estado={run.state.life_cycle_state.value} resultado={run.state.result_state}")
    print(f"mensagem={run.state.state_message} duração={time.monotonic() - inicio:.0f}s")
    for t in run.tasks or []:
        print(
            f"- {t.task_key}: {t.state.result_state} run_id={t.run_id} "
            f"execução={(t.execution_duration or 0) / 1000:.0f}s"
        )
        if t.state.result_state != jobs.RunResultState.SUCCESS:
            saida = w.jobs.get_run_output(t.run_id)
            print(saida.error, "\n", (saida.error_trace or "")[-4000:])
            print((saida.logs or "")[-4000:])


def main(alvo: str) -> None:
    w = WorkspaceClient()
    if alvo == "wheel":
        (whl,) = (AQUI.parent / "dist").glob("*.whl")
        with whl.open("rb") as arquivo:
            w.files.upload(WHEEL, arquivo, overwrite=True)
        print(f"{whl.name} → {WHEEL}")
        return
    pasta = subir_scripts(w)
    if alvo == "sondagem":
        submeter(w, "brasileirao-sondagem", [task("sondagem", f"{pasta}/sondagem.py", [VOLUME])])
    elif alvo == "pipeline":
        submeter(
            w,
            "brasileirao-pipeline",
            [
                task("lakehouse", f"{pasta}/executar.py", ["lakehouse", "--todas"]),
                task("gold", f"{pasta}/executar.py", ["gold"], depois="lakehouse"),
                task("foto", f"{pasta}/foto.py", [VOLUME, f"{VOLUME}/foto.json"], depois="gold"),
            ],
        )
    else:
        raise SystemExit(f"alvo desconhecido: {alvo!r} (use sondagem, pipeline ou wheel)")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "")
