"""Planos físicos de algumas tabelas gold em texto, para a leitura comentada em `docs/explain/`."""

import io
import logging
from contextlib import redirect_stdout
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from brasileirao import config
from brasileirao.aggregate import gold

log = logging.getLogger(__name__)

# A classificação (windows + exchange), as sequências (duas windows na mesma partição) e a
# disciplina, que tem o `BroadcastHashJoin` explícito.
PLANOS = ("classificacao_por_rodada", "sequencias", "disciplina")
DESTINO_PADRAO = "docs/explain/planos"


def plano(df: DataFrame) -> str:
    """`explain("formatted")` depois de executar a consulta.

    Antes da ação, o AQE só mostra o plano inicial (`isFinalPlan=false`). Depois, o mesmo
    `explain` traz o plano final e o inicial. O `collect()` é o que executa a própria consulta
    do DataFrame (`count()` ou escrita montariam outra); as tabelas gold têm no máximo
    algumas dezenas de milhares de linhas.
    """
    df.collect()
    with redirect_stdout(io.StringIO()) as saida:
        df.explain("formatted")
    return saida.getvalue()


def gerar_planos(spark: SparkSession, destino: str = DESTINO_PADRAO) -> list[Path]:
    """Calcula as tabelas de `PLANOS` pelo registro do gold e grava `<destino>/<tabela>.txt`.

    O caminho absoluto da raiz dos dados vira `$BRASILEIRAO_DATA_ROOT`, para o texto
    versionado não depender da máquina.
    """
    fontes = gold.Fontes(spark)
    pasta = Path(destino)
    pasta.mkdir(parents=True, exist_ok=True)
    raiz = f"file:{Path(config.data_root()).resolve()}"
    arquivos = []
    for nome in PLANOS:
        tabela = gold.TABELAS[nome]
        df = tabela.calcular(*[fontes[e] for e in tabela.entradas])
        arquivo = pasta / f"{nome}.txt"
        arquivo.write_text(plano(df).replace(raiz, "$BRASILEIRAO_DATA_ROOT"), encoding="utf-8")
        log.info("plano de gold/%s em %s", nome, arquivo)
        arquivos.append(arquivo)
    return arquivos
