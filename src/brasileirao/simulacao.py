"""Simulação de temporada em andamento: a fonte de 2025 chegando rodada a rodada.

Não existe temporada real em andamento na fonte (a versão 18 termina em 07/12/2025), então a
simulação monta, para a rodada N, um retrato dos CSVs só com o que já teria sido publicado:

- partidas da temporada com `rodata` até N;
- menos as adiadas de `reference/simulacao_eventos.csv` que ainda não foram jogadas (o evento
  diz em que rodada da simulação cada uma aparece);
- com o placar provisório dos eventos `placar_corrigido` até a rodada em que a correção chega.

As filhas (gols, cartões, estatísticas) seguem as partidas do retrato. Tudo é gravado num lake
isolado (`data_sim` por padrão), e o pipeline roda nele com a opção de temporada em andamento.
O `./data` e o `app/dados` nunca são escritos.

`python -m brasileirao.simulacao preparar [--historico]`, `... rodada N [--ate M]` e
`... relatorio N`.
"""

import argparse
import csv
import io
import logging
import os
import shutil
import time
from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from brasileirao import config, pipeline
from brasileirao.ingestion.bronze import ARQUIVOS
from brasileirao.lake import cdf, historico

log = logging.getLogger(__name__)

DESTINO_PADRAO = "data_sim"
FONTE_PADRAO = "data/raw"
TEMPORADA_PADRAO = 2025
TIPOS_EVENTO = ("adiado", "placar_corrigido")
# Rótulo de cada commit da rodada no histórico do Delta: é por ele que o relatório acha as
# versões de `silver/partidas` que a rodada criou.
METADADO = "spark.databricks.delta.commitInfo.userMetadata"
TABELA_RELATORIO = "silver/partidas"
# Colunas do CSV de partidas usadas pelo retrato.
ID, RODADA, DATA, VENCEDOR, PLACAR_M, PLACAR_V = 0, 1, 2, 10, 12, 13
PROIBIDOS = ("data", "app/dados")


class SimulacaoError(ValueError):
    pass


@dataclass(frozen=True)
class Evento:
    tipo: str
    partida_id: str
    rodada_publicacao: int
    placar_mandante: str
    placar_visitante: str
    descricao: str


@dataclass
class Retrato:
    rodada: int
    partidas: int
    adiadas_pendentes: list[str] = field(default_factory=list)
    placares_provisorios: list[str] = field(default_factory=list)


def rotulo(temporada: int, rodada: int) -> str:
    return f"simulacao {temporada} rodada {rodada}"


def carregar_eventos(texto: str | None = None) -> list[Evento]:
    """Eventos de `reference/simulacao_eventos.csv` (ou de `texto`, nos testes)."""
    if texto is None:
        arquivo = resources.files("brasileirao.reference").joinpath("simulacao_eventos.csv")
        texto = arquivo.read_text("utf-8")
    eventos = []
    for linha in csv.DictReader(io.StringIO(texto)):
        if linha["tipo"] not in TIPOS_EVENTO:
            raise SimulacaoError(f"evento de tipo desconhecido: {linha['tipo']!r}")
        evento = Evento(
            linha["tipo"],
            linha["partida_id"],
            int(linha["rodada_publicacao"]),
            linha["placar_mandante"],
            linha["placar_visitante"],
            linha["descricao"],
        )
        if evento.tipo == "placar_corrigido" and not (
            evento.placar_mandante.isdigit() and evento.placar_visitante.isdigit()
        ):
            raise SimulacaoError(f"placar provisório inválido no evento da {evento.partida_id}")
        eventos.append(evento)
    return eventos


def validar_destino(destino: str) -> Path:
    """Recusa o lake normal (`./data`), o `app/dados` e caminho remoto como destino."""
    if "://" in destino:
        raise SimulacaoError(f"a simulação só grava em disco local, não em {destino}")
    alvo = Path(destino).resolve()
    for proibido in PROIBIDOS:
        raiz = Path(proibido).resolve()
        if alvo == raiz or raiz in alvo.parents or alvo in raiz.parents:
            raise SimulacaoError(
                f"destino {destino} recusado: a simulação nunca escreve em ./{proibido}; "
                f"use BRASILEIRAO_DATA_ROOT={DESTINO_PADRAO}"
            )
    return alvo


def temporada_da_data(data: str) -> int:
    """A mesma regra de `utils.temporada.derivar_temporada`, sobre o texto `dd/MM/yyyy`."""
    _, mes, ano = (int(p) for p in data.split("/"))
    return 2020 if ano == 2021 and mes <= 3 else ano


def campos_crus(linha: str) -> list[str]:
    """Campos de uma linha do CSV como estão no arquivo, com as aspas.

    O retrato copia as linhas sem reescrever: aspas vazias e campo vazio sem aspas chegam
    iguais ao Spark. Só os campos trocados pelo evento são montados de novo.
    """
    campos, atual, entre_aspas = [], [], False
    for caractere in linha:
        if caractere == '"':
            entre_aspas = not entre_aspas
        if caractere == "," and not entre_aspas:
            campos.append("".join(atual))
            atual = []
        else:
            atual.append(caractere)
    campos.append("".join(atual))
    return campos


def _valor(campo: str) -> str:
    return next(csv.reader([campo]))[0] if campo else ""


def _citar(valor: str) -> str:
    return '"' + valor.replace('"', '""') + '"'


def gerar_retrato(
    fonte: Path, destino_raw: Path, temporada: int, rodada: int, eventos: list[Evento]
) -> Retrato:
    """Grava em `destino_raw` os quatro CSVs da fonte como estariam depois da `rodada`.

    No placar provisório ficam só os primeiros gols de cada clube, na ordem da fonte, até o
    placar publicado; os outros chegam com a correção.

    `rodada=0` tira a temporada inteira (o lake antes da primeira rodada).
    """
    adiadas = {e.partida_id: e for e in eventos if e.tipo == "adiado"}
    provisorios = {e.partida_id: e for e in eventos if e.tipo == "placar_corrigido"}
    destino_raw.mkdir(parents=True, exist_ok=True)
    retrato = Retrato(rodada, 0)
    ocultas: set[str] = set()
    # Partida com placar provisório → gols que cabem nele, por clube: o gol que falta no
    # placar também falta em `gols`, senão o gold (viradas) acusa placar ≠ gols, com razão.
    cota: dict[str, dict[str, int]] = {}
    with (
        (fonte / ARQUIVOS["partidas"]).open(encoding="utf-8", newline="") as entrada,
        (destino_raw / ARQUIVOS["partidas"]).open("w", encoding="utf-8", newline="") as saida,
    ):
        saida.write(next(entrada))
        for linha in entrada:
            campos = campos_crus(linha.rstrip("\r\n"))
            partida_id, rodata = _valor(campos[ID]), int(_valor(campos[RODADA]))
            if temporada_da_data(_valor(campos[DATA])) != temporada:
                saida.write(linha)
                continue
            adiada = adiadas.get(partida_id)
            if rodata > rodada or (adiada and adiada.rodada_publicacao > rodada):
                ocultas.add(partida_id)
                if adiada and rodata <= rodada:
                    retrato.adiadas_pendentes.append(partida_id)
                continue
            evento = provisorios.get(partida_id)
            if evento and evento.rodada_publicacao > rodada:
                linha = _com_placar(campos, evento) + linha[len(linha.rstrip("\r\n")) :]
                retrato.placares_provisorios.append(partida_id)
                cota[partida_id] = {
                    _valor(campos[4]): int(evento.placar_mandante),
                    _valor(campos[5]): int(evento.placar_visitante),
                }
            saida.write(linha)
            retrato.partidas += 1
    for entidade in ("gols", "cartoes", "estatisticas"):
        with (
            (fonte / ARQUIVOS[entidade]).open(encoding="utf-8", newline="") as entrada,
            (destino_raw / ARQUIVOS[entidade]).open("w", encoding="utf-8", newline="") as saida,
        ):
            saida.write(next(entrada))
            for linha in entrada:
                campos = campos_crus(linha)
                partida_id = _valor(campos[0])
                if partida_id in ocultas:
                    continue
                if entidade == "gols" and partida_id in cota:
                    clube = _valor(campos[2])
                    if cota[partida_id].get(clube, 0) <= 0:
                        continue
                    cota[partida_id][clube] -= 1
                saida.write(linha)
    sobrando = sorted(p for p, clubes in cota.items() if any(clubes.values()))
    if sobrando:
        raise SimulacaoError(
            f"placar provisório com mais gols que a fonte nas partidas {', '.join(sobrando)}: "
            "o evento só pode tirar gol, não inventar"
        )
    return retrato


def _com_placar(campos: list[str], evento: Evento) -> str:
    """A linha com o placar provisório e o vencedor que ele daria (`-` no empate)."""
    mandante, visitante = int(evento.placar_mandante), int(evento.placar_visitante)
    if mandante == visitante:
        vencedor = "-"
    else:
        vencedor = _valor(campos[4] if mandante > visitante else campos[5])
    novos = list(campos)
    novos[PLACAR_M] = _citar(evento.placar_mandante)
    novos[PLACAR_V] = _citar(evento.placar_visitante)
    novos[VENCEDOR] = _citar(vencedor)
    return ",".join(novos)


@dataclass
class ExecucaoRodada:
    retrato: Retrato
    segundos_retrato: float
    segundos_lakehouse: float
    segundos_gold: float | None


def executar_rodada(
    spark: SparkSession,
    rodada: int,
    temporada: int = TEMPORADA_PADRAO,
    fonte: Path = Path(FONTE_PADRAO),
    eventos: list[Evento] | None = None,
    gold: bool = True,
) -> ExecucaoRodada:
    """Retrato da rodada → bronze/silver da temporada → gold, no lake de `BRASILEIRAO_DATA_ROOT`.

    Os commits da rodada levam o rótulo `simulacao <temporada> rodada <N>` no histórico.
    """
    raiz = config.data_root()
    validar_destino(raiz)
    eventos = carregar_eventos() if eventos is None else eventos
    inicio = time.monotonic()
    retrato = gerar_retrato(fonte, Path(raiz) / "raw", temporada, rodada, eventos)
    meio = time.monotonic()
    spark.conf.set(METADADO, rotulo(temporada, rodada))
    try:
        pipeline.lakehouse(spark, temporada, aceitar_em_andamento=True)
        fim_lakehouse = time.monotonic()
        segundos_gold = None
        if gold:
            pipeline.executar_gold(spark, aceitar_em_andamento=True)
            segundos_gold = time.monotonic() - fim_lakehouse
    finally:
        spark.conf.unset(METADADO)
    return ExecucaoRodada(retrato, meio - inicio, fim_lakehouse - meio, segundos_gold)


def preparar(spark: SparkSession, temporada: int, fonte: Path, com_historico: bool) -> None:
    """Apaga o lake da simulação e, com `com_historico`, carrega as outras temporadas."""
    raiz = config.data_root()
    validar_destino(raiz)
    if Path(raiz).exists():
        shutil.rmtree(raiz)
    gerar_retrato(fonte, Path(raiz) / "raw", temporada, 0, carregar_eventos())
    if com_historico:
        spark.conf.set(METADADO, rotulo(temporada, 0))
        try:
            pipeline.lakehouse(spark, None)
        finally:
            spark.conf.unset(METADADO)


def versoes_da_rodada(
    spark: SparkSession, rodada: int, temporada: int = TEMPORADA_PADRAO
) -> list[int]:
    """Versões de `silver/partidas` gravadas pela rodada, pelo rótulo do commit."""
    caminho = config.caminho_tabela("silver", "partidas")
    if not DeltaTable.isDeltaTable(spark, caminho):
        return []
    linhas = (
        DeltaTable.forPath(spark, caminho)
        .history()
        .where(F.col("userMetadata") == rotulo(temporada, rodada))
        .select("version")
        .collect()
    )
    return sorted(r["version"] for r in linhas)


def _texto(valor) -> str:
    return "nulo" if valor is None else str(valor)


def _partida(r) -> str:
    return (
        f"#{r['partida_id']} rodada {r['rodada']} {r['mandante']} {r['placar_mandante']} x "
        f"{r['placar_visitante']} {r['visitante']}"
    )


def relatorio(spark: SparkSession, rodada: int, temporada: int = TEMPORADA_PADRAO) -> str:
    """O que entrou, mudou e saiu de `silver/partidas` da rodada N−1 para a N, lido do Change
    Data Feed e conferido com a comparação de versões (`lake.historico`)."""
    versoes = versoes_da_rodada(spark, rodada, temporada)
    if not versoes:
        return f"rodada {rodada}: nenhuma versão nova em {TABELA_RELATORIO}"
    inicio, fim = versoes[0], versoes[-1]
    feed = cdf.mudancas(spark, TABELA_RELATORIO, inicio, fim).collect()
    por_tipo: dict[str, list] = {}
    for linha in feed:
        por_tipo.setdefault(linha["_change_type"], []).append(linha)
    entraram = sorted(por_tipo.get("insert", []), key=lambda r: (r["rodada"], r["partida_id"]))
    sairam = sorted(por_tipo.get("delete", []), key=lambda r: r["partida_id"])
    antes = {
        (r["partida_id"], r["_commit_version"]): r for r in por_tipo.get("update_preimage", [])
    }
    depois = sorted(
        por_tipo.get("update_postimage", []), key=lambda r: (r["partida_id"], r["_commit_version"])
    )
    linhas = [
        f"rodada {rodada}: {TABELA_RELATORIO} versões {inicio} a {fim} (Change Data Feed)",
        f"entraram {len(entraram)} partida(s)",
    ]
    for r in entraram:
        atrasada = " (de rodada anterior, chegou agora)" if r["rodada"] < rodada else ""
        linhas.append(f"  {_partida(r)}{atrasada}")
    linhas.append(f"mudaram {len(depois)} partida(s)")
    colunas = [c for c in (depois[0].asDict() if depois else {}) if not c.startswith("_")]
    for r in depois:
        velho = antes[(r["partida_id"], r["_commit_version"])]
        diferencas = ", ".join(
            f"{c} {_texto(velho[c])} → {_texto(r[c])}" for c in colunas if velho[c] != r[c]
        )
        linhas.append(f"  {_partida(r)}: {diferencas}")
    linhas.append(f"saíram {len(sairam)} partida(s)")
    linhas += [f"  {_partida(r)}" for r in sairam]
    if inicio == 0:
        # A rodada criou a tabela: não há versão anterior, tudo o que existe entrou agora.
        conferencia = (historico.ler_versao(spark, TABELA_RELATORIO, fim).count(), 0, 0)
    else:
        comparacao = historico.comparar_versoes(
            spark, TABELA_RELATORIO, inicio - 1, fim, chave=["partida_id"]
        )
        conferencia = (
            comparacao.inseridas.count(),
            comparacao.alteradas.count(),
            comparacao.removidas.count(),
        )
    bate = conferencia == (len(entraram), len(depois), len(sairam))
    linhas.append(
        f"conferência com a comparação de versões ({max(inicio - 1, 0)} → {fim}): "
        f"entraram {conferencia[0]}, "
        f"mudaram {conferencia[1]}, saíram {conferencia[2]}: "
        + ("bate com o feed" if bate else "NÃO bate com o feed")
    )
    return "\n".join(linhas)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m brasileirao.simulacao")
    parser.add_argument("--destino", default=DESTINO_PADRAO, help="lake da simulação")
    parser.add_argument("--fonte", default=FONTE_PADRAO, help="CSVs completos da fonte")
    parser.add_argument("--temporada", type=int, default=TEMPORADA_PADRAO)
    comandos = parser.add_subparsers(dest="comando", required=True)
    cmd = comandos.add_parser("preparar", help="apaga o lake da simulação")
    cmd.add_argument(
        "--historico", action="store_true", help="carrega as outras temporadas da fonte"
    )
    cmd = comandos.add_parser("rodada", help="processa a rodada N (ou de N até --ate)")
    cmd.add_argument("n", type=int)
    cmd.add_argument("--ate", type=int, help="última rodada, em sequência")
    cmd.add_argument("--sem-gold", action="store_true", help="só bronze e silver")
    cmd = comandos.add_parser("relatorio", help="o que mudou na silver com a rodada N")
    cmd.add_argument("n", type=int)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    try:
        validar_destino(args.destino)
    except SimulacaoError as erro:
        raise SystemExit(f"Erro: {erro}") from erro
    os.environ["BRASILEIRAO_DATA_ROOT"] = args.destino
    from brasileirao.utils.spark import get_spark

    spark = get_spark()
    fonte = Path(args.fonte)
    if args.comando == "preparar":
        preparar(spark, args.temporada, fonte, args.historico)
    elif args.comando == "relatorio":
        print(relatorio(spark, args.n, args.temporada))
    else:
        eventos = carregar_eventos()
        for rodada in range(args.n, (args.ate or args.n) + 1):
            execucao = executar_rodada(
                spark, rodada, args.temporada, fonte, eventos, gold=not args.sem_gold
            )
            r = execucao.retrato
            gold = "-" if execucao.segundos_gold is None else f"{execucao.segundos_gold:.1f}s"
            print(
                f"rodada {rodada}: {r.partidas} partidas de {args.temporada} no retrato, "
                f"adiadas pendentes {r.adiadas_pendentes or '-'}, "
                f"placar provisório {r.placares_provisorios or '-'}; "
                f"retrato {execucao.segundos_retrato:.1f}s, "
                f"lakehouse {execucao.segundos_lakehouse:.1f}s, gold {gold}"
            )
            print(relatorio(spark, rodada, args.temporada))


if __name__ == "__main__":
    main()
