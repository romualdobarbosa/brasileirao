import copy
from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag
from airflow.sdk.definitions.param import ParamValidationError

from brasileirao import orquestracao

PASTA_DAGS = Path(__file__).parent.parent / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(PASTA_DAGS))


@pytest.fixture(scope="module")
def dag(dagbag):
    return dagbag.dags[orquestracao.DAG_ID]


def test_importa_sem_erro(dagbag):
    assert dagbag.import_errors == {}
    assert sorted(dagbag.dag_ids) == [orquestracao.DAG_ID]


def test_sem_ciclos(dag):
    dag.check_cycle()


def test_ordem_das_tarefas(dag):
    esperado = [t for t, _ in orquestracao.ETAPAS] + [orquestracao.TAREFA_REGISTRO]
    assert [t.task_id for t in dag.topological_sort()] == esperado
    for anterior, seguinte in zip(esperado, esperado[1:], strict=False):
        assert dag.get_task(seguinte).upstream_task_ids == {anterior}
    # Regra padrão: falhou uma, as seguintes ficam em upstream_failed.
    assert {t.trigger_rule for t in dag.tasks} == {"all_success"}


def test_parametros(dag):
    assert set(dag.params) == {"rodada", "alvo"}
    assert dag.params["alvo"] == "local"
    assert dag.params.get_param("alvo").schema["enum"] == ["local", "aws"]
    assert dag.schedule is None
    assert dag.max_active_runs == 1


@pytest.mark.parametrize(
    "conf", [{"rodada": 0}, {"rodada": 39}, {"rodada": "1; rm -rf /"}, {"rodada": 1, "alvo": "x"}]
)
def test_parametro_invalido_e_recusado(dag, conf):
    with pytest.raises(ParamValidationError):
        _validar(dag, conf)


def test_parametro_valido(dag):
    assert _validar(dag, {"rodada": 38, "alvo": "aws"}) == {"rodada": 38, "alvo": "aws"}
    assert _validar(dag, {"rodada": 1}) == {"rodada": 1, "alvo": "local"}
    # Sem rodada não há padrão: o disparo precisa dizer qual.
    with pytest.raises(ParamValidationError):
        _validar(dag, {})


def _validar(dag, conf: dict) -> dict:
    params = copy.deepcopy(dag.params)
    params.update(conf)
    return params.validate()


def test_tarefas_do_spark_usam_o_ambiente(dag):
    for tarefa, etapa in orquestracao.ETAPAS:
        operador = dag.get_task(tarefa)
        assert operador.bash_command == orquestracao.comando(etapa)
        assert operador.env == {"RODADA": "{{ params.rodada }}", "ALVO": "{{ params.alvo }}"}
        assert operador.cwd == orquestracao.PASTA_PROJETO
