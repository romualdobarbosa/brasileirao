import json
from pathlib import Path

from brasileirao.aggregate import gold
from brasileirao.transform import silver

TABELAS_DELTA = Path(__file__).parents[2] / "infra" / "main" / "tabelas_delta.json"


def test_glue_registra_as_tabelas_que_o_pipeline_grava():
    """O Terraform do Glue lê esta lista; tabela nova no pipeline tem de entrar nela."""
    tabelas = json.loads(TABELAS_DELTA.read_text())

    assert set(tabelas) == {"silver", "gold"}
    assert sorted(tabelas["silver"]) == sorted(silver.TABELAS)
    assert sorted(tabelas["gold"]) == sorted(gold.TABELAS)
