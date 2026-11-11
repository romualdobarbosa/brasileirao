"""Ponto de entrada das tasks do job: `executar.py <comando do pipeline> [args...]`.

Fixa o ambiente antes de importar o pacote (o wheel vem do environment do job) e chama a
mesma CLI do local, `python -m brasileirao.pipeline`.
"""

import os
import sys

os.environ.setdefault("BRASILEIRAO_SPARK_ENV", "databricks")
os.environ.setdefault("BRASILEIRAO_DATA_ROOT", "/Volumes/workspace/default/brasileirao")

from brasileirao.pipeline import main  # noqa: E402

main(sys.argv[1:])
