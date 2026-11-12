"""Registro das execuções na tabela Delta `ops/execucoes`, e o `make status` que lê ela.

Cada execução da CLI (`python -m brasileirao.pipeline` e `python -m brasileirao.simulacao`)
grava uma linha no lake do próprio processo (`$BRASILEIRAO_DATA_ROOT/ops/execucoes`): `data/`
na execução normal, `data_sim/` na simulação. A linha sai inclusive quando a execução falha,
com o erro entre os avisos.

O registro é o último passo e nunca muda o resultado: se gravar falhar, fica só um aviso no
log, e a execução termina como terminaria sem ele (mesmo erro, mesmo código de saída).

`python -m brasileirao.execucoes [--limite N]` mostra as últimas execuções (`make status`).
"""

import argparse
import logging
import shlex
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime

from brasileirao import config

log = logging.getLogger(__name__)

TABELA = "ops/execucoes"
SCHEMA = (
    "id string, comando string, alvo string, inicio timestamp, fim timestamp, status string, "
    "linhas map<string, bigint>, avisos array<string>"
)
SUCESSO, FALHA = "sucesso", "falha"
# Aviso longo (lista de partidas, traceback) é cortado; a lista também tem limite.
MAX_AVISOS = 50
MAX_TEXTO = 1000

_atual: ContextVar["Execucao | None"] = ContextVar("execucao_atual", default=None)


def caminho() -> str:
    return f"{config.data_root()}/{TABELA}"


@dataclass
class Execucao:
    comando: str
    alvo: str
    inicio: datetime
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    fim: datetime | None = None
    status: str | None = None
    linhas: dict[str, int] = field(default_factory=dict)
    avisos: list[str] = field(default_factory=list)

    def avisar(self, texto: str) -> None:
        if len(self.avisos) < MAX_AVISOS:
            self.avisos.append(texto[:MAX_TEXTO])
        elif len(self.avisos) == MAX_AVISOS:
            self.avisos.append("... avisos demais, o resto está só no log")


def contar(tabela: str, linhas: int) -> None:
    """Soma `linhas` gravadas em `tabela` na execução em andamento (sem execução, nada)."""
    execucao = _atual.get()
    if execucao is not None:
        execucao.linhas[tabela] = execucao.linhas.get(tabela, 0) + int(linhas)


class _ColetorDeAvisos(logging.Handler):
    """Guarda na execução os avisos (WARNING ou mais) logados durante ela."""

    def __init__(self, execucao: Execucao) -> None:
        super().__init__(logging.WARNING)
        self.execucao = execucao

    def emit(self, record: logging.LogRecord) -> None:
        # O aviso do próprio registro não entra: ele é sobre a gravação desta linha.
        if record.name != __name__:
            self.execucao.avisar(f"{record.name}: {record.getMessage()}")


def comando_de(programa: str, argv: list[str]) -> str:
    return shlex.join([programa, *argv])


def gravar_delta(execucao: Execucao) -> None:
    """Acrescenta a linha em `ops/execucoes`, na sessão Spark aberta (ou numa nova)."""
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        from brasileirao.utils.spark import get_spark

        spark = get_spark()
    linha = (
        execucao.id,
        execucao.comando,
        execucao.alvo,
        execucao.inicio,
        execucao.fim,
        execucao.status,
        execucao.linhas,
        execucao.avisos,
    )
    spark.createDataFrame([linha], SCHEMA).write.format("delta").mode("append").save(caminho())


def _erro(excecao: BaseException) -> str:
    if isinstance(excecao, SystemExit):
        return excecao.code if isinstance(excecao.code, str) else f"Erro: saída {excecao.code}"
    return f"Erro: {type(excecao).__name__}: {excecao}"


@contextmanager
def registrar(
    comando: str, gravar: Callable[[Execucao], None] = gravar_delta
) -> Iterator[Execucao]:
    """Registra o bloco como uma execução, com sucesso ou falha, e repassa o resultado dele.

    `SystemExit` com código 0 (ou sem código) é sucesso; qualquer outra exceção é falha.
    """
    execucao = Execucao(comando, config.spark_env(), datetime.now(UTC))
    coletor = _ColetorDeAvisos(execucao)
    raiz = logging.getLogger()
    raiz.addHandler(coletor)
    token = _atual.set(execucao)
    try:
        yield execucao
    except BaseException as excecao:
        if isinstance(excecao, SystemExit) and excecao.code in (0, None):
            execucao.status = SUCESSO
        else:
            execucao.status = FALHA
            execucao.avisar(_erro(excecao))
        raise
    else:
        execucao.status = SUCESSO
    finally:
        _atual.reset(token)
        raiz.removeHandler(coletor)
        execucao.fim = datetime.now(UTC)
        try:
            gravar(execucao)
            log.info("execução %s registrada em %s: %s", execucao.id, TABELA, execucao.status)
        except Exception as erro:  # o registro nunca derruba a execução
            log.warning(
                "execução %s não registrada em %s (%s: %s); o resultado dela não muda",
                execucao.id,
                caminho(),
                type(erro).__name__,
                erro,
            )


def _hora(valor: datetime | None) -> str:
    return valor.strftime("%Y-%m-%d %H:%M:%S") if valor else "-"


def ultimas(spark, limite: int = 10) -> list:
    """As `limite` execuções mais recentes, da mais nova para a mais velha."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, caminho()):
        return []
    tabela = spark.read.format("delta").load(caminho())
    return tabela.orderBy(tabela["inicio"].desc()).head(limite)


def formatar(linhas: list) -> str:
    """O texto do `make status`: uma linha por execução e, embaixo, linhas e avisos."""
    if not linhas:
        return f"nenhuma execução registrada em {caminho()}"
    saida = [f"últimas {len(linhas)} execução(ões) em {caminho()}"]
    for r in linhas:
        segundos = (r["fim"] - r["inicio"]).total_seconds() if r["fim"] else None
        duracao = f"{segundos:.0f}s" if segundos is not None else "-"
        total = sum((r["linhas"] or {}).values())
        saida.append(
            f"{_hora(r['inicio'])}  {r['status']:<7}  {duracao:>5}  {r['alvo']:<5}  "
            f"{r['id'][:8]}  {r['comando']}"
        )
        if r["linhas"]:
            tabelas = ", ".join(f"{t} {n}" for t, n in sorted(r["linhas"].items()))
            saida.append(f"    linhas: {total} ({tabelas})")
        for aviso in r["avisos"] or []:
            primeira, *resto = aviso.splitlines() or [""]
            saida.append(f"    aviso: {primeira}")
            saida += [f"           {linha}" for linha in resto]
    return "\n".join(saida)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m brasileirao.execucoes")
    parser.add_argument("--limite", type=int, default=10, help="quantas execuções mostrar")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    from brasileirao.utils.spark import get_spark

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    print(formatar(ultimas(spark, args.limite)))


if __name__ == "__main__":
    main()
