"""Testes da DAG, num ambiente com o Airflow (`make test-dag`), fora da suíte principal.

O Airflow lê a configuração na importação: o AIRFLOW_HOME vai para uma pasta temporária antes,
para o teste não criar ~/airflow.
"""

import os
import tempfile

os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow-home-"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
