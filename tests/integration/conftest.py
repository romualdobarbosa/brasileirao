import shutil
from pathlib import Path

import pytest

from brasileirao import pipeline

FIXTURES_RAW_GOLD = Path(__file__).parent.parent / "fixtures" / "raw_gold"


@pytest.fixture(scope="module")
def silver(spark, tmp_path_factory):
    """Silver das fixtures `raw_gold` (o minicampeonato em 2019), montada uma vez por módulo."""
    raiz = tmp_path_factory.mktemp("lake")
    shutil.copytree(FIXTURES_RAW_GOLD, raiz / "raw")
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("BRASILEIRAO_DATA_ROOT", str(raiz))
        assert pipeline.lakehouse(spark, None) == [2019]
        yield raiz
