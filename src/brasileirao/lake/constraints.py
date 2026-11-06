"""Constraints `CHECK` das tabelas silver e gold, aplicadas de forma idempotente.

São a última linha de defesa: valem para qualquer escrita na tabela, venha do pipeline ou não.
Só entram regras de uma linha por vez. O que precisa de join ou agregação fica em `qualidade.py`,
e as checagens de lá continuam rodando antes da escrita.

Duas regras do Delta moldam este módulo. Um `CHECK` que dá `NULL` recusa a linha, ao contrário do
SQL padrão, então toda coluna opcional precisa de `IS NULL OR`. E o Delta remonta o texto que
guarda, pondo espaço em volta de parêntese e vírgula, então a comparação com o registro ignora
os espaços (`mesma_expressao`).
"""

import logging
import os

from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession

from brasileirao import config
from brasileirao.aggregate.esquemas import PRIMEIRA_TEMPORADA as PRIMEIRA_TEMPORADA_ESQUEMAS
from brasileirao.aggregate.goleadas import DIFERENCA_MINIMA, TOTAL_MINIMO
from brasileirao.aggregate.gols_por_minuto import FAIXAS, LADOS
from brasileirao.aggregate.sequencias import TIPOS
from brasileirao.aggregate.viradas import PRIMEIRA_TEMPORADA_GOLS
from brasileirao.transform.qualidade import CARTOES_VALIDOS

log = logging.getLogger(__name__)

PREFIXO_PROPRIEDADE = "delta.constraints."
PRIMEIRA_TEMPORADA_FONTE = 2003
# 24 times em 2003 e 2004: 23 × 2 rodadas.
MAIOR_RODADA = 46


def _em(coluna: str, valores) -> str:
    return f"{coluna} IN ({', '.join(repr(v) for v in valores)})"


def _obrigatorio(coluna: str, regra: str) -> str:
    return f"{coluna} IS NOT NULL AND {regra}"


def _opcional(coluna: str, regra: str) -> str:
    return f"{coluna} IS NULL OR {regra}"


def _nao_negativo(*colunas: str) -> str:
    return " AND ".join(f"{c} >= 0" for c in colunas)


def _nao_negativo_opcional(*colunas: str) -> str:
    return " AND ".join(f"({_opcional(c, f'{c} >= 0')})" for c in colunas)


TEMPORADA = {"temporada_valida": f"temporada >= {PRIMEIRA_TEMPORADA_FONTE}"}
RODADA = {"rodada_valida": f"rodada BETWEEN 1 AND {MAIOR_RODADA}"}
JOGOS_FECHAM = {"jogos_fecham": "jogos = vitorias + empates + derrotas"}
APROVEITAMENTO = {"aproveitamento_valido": "aproveitamento BETWEEN 0 AND 1"}
POSICAO = {"posicao_valida": "posicao >= 1"}
PARTIDA_ID = {"partida_id_nao_nulo": "partida_id IS NOT NULL"}
MINUTO = {
    "minuto_valido": _opcional("minuto", "minuto >= 0"),
    "acrescimo_valido": _opcional("acrescimo", "acrescimo >= 0"),
}
MANDO = {"mando_valido": _em("mando", LADOS)}

CONSTRAINTS: dict[str, dict[str, str]] = {
    "silver/partidas": {
        **PARTIDA_ID,
        **TEMPORADA,
        **RODADA,
        "placar_mandante_valido": _obrigatorio("placar_mandante", "placar_mandante >= 0"),
        "placar_visitante_valido": _obrigatorio("placar_visitante", "placar_visitante >= 0"),
    },
    "silver/gols": {**PARTIDA_ID, **TEMPORADA, **RODADA, **MINUTO},
    "silver/cartoes": {
        **PARTIDA_ID,
        **TEMPORADA,
        **RODADA,
        **MINUTO,
        "cartao_valido": _obrigatorio("cartao", _em("cartao", CARTOES_VALIDOS)),
        "num_camisa_valido": _opcional("num_camisa", "num_camisa >= 0"),
    },
    "silver/estatisticas": {
        **PARTIDA_ID,
        **TEMPORADA,
        **RODADA,
        "contagens_nao_negativas": _nao_negativo_opcional(
            "chutes",
            "chutes_no_alvo",
            "passes",
            "faltas",
            "cartao_amarelo",
            "cartao_vermelho",
            "impedimentos",
            "escanteios",
        ),
        "posse_de_bola_valida": _opcional("posse_de_bola", "posse_de_bola BETWEEN 0 AND 1"),
        "precisao_passes_valida": _opcional("precisao_passes", "precisao_passes BETWEEN 0 AND 1"),
    },
    "gold/classificacao_por_rodada": {
        **TEMPORADA,
        **RODADA,
        **POSICAO,
        **JOGOS_FECHAM,
        **APROVEITAMENTO,
        "saldo_fecha": "saldo = gols_pro - gols_contra",
    },
    "gold/desempenho_mandante_visitante": {
        **TEMPORADA,
        **MANDO,
        **JOGOS_FECHAM,
        **APROVEITAMENTO,
    },
    "gold/sequencias": {
        **TEMPORADA,
        "tipo_valido": _em("tipo", TIPOS),
        # Clube sem série do tipo fica com 0 jogos e datas nulas (`calcular_sequencias`).
        "jogos_nao_negativos": _nao_negativo("jogos"),
        "datas_em_ordem": _opcional("data_inicio", "data_inicio <= data_fim"),
    },
    "gold/ranking_historico": {
        **POSICAO,
        **JOGOS_FECHAM,
        **APROVEITAMENTO,
        "temporadas_em_ordem": "primeira_temporada <= ultima_temporada",
    },
    "gold/disciplina": {
        **TEMPORADA,
        "cartoes_nao_negativos": _nao_negativo("amarelos", "vermelhos"),
        "fonte_valida": _em("fonte", ("cartoes", "estatisticas")),
    },
    "gold/artilharia": {
        **TEMPORADA,
        **POSICAO,
        "gols_positivos": "gols >= 1",
        "penaltis_validos": "penaltis BETWEEN 0 AND gols",
    },
    "gold/gols_por_minuto": {
        **TEMPORADA,
        "faixa_valida": _em("faixa", [faixa for faixa, _ in FAIXAS]),
        "lado_valido": _em("lado", LADOS),
        "gols_nao_negativos": _nao_negativo("gols"),
        "pct_gols_valido": "pct_gols BETWEEN 0 AND 1",
    },
    "gold/resumo_temporada": {
        **TEMPORADA,
        "partidas_positivas": "partidas >= 1",
        "gols_nao_negativos": _nao_negativo("gols"),
    },
    "gold/confrontos": {
        **JOGOS_FECHAM,
        **APROVEITAMENTO,
        "adversario_diferente": "clube <> adversario",
    },
    "gold/goleadas": {
        **TEMPORADA,
        **RODADA,
        "criterio_de_goleada": f"diferenca >= {DIFERENCA_MINIMA} OR total_gols >= {TOTAL_MINIMO}",
        "placar_nao_negativo": _nao_negativo("placar_mandante", "placar_visitante"),
    },
    "gold/campanhas": {
        **TEMPORADA,
        **POSICAO,
        **JOGOS_FECHAM,
        **APROVEITAMENTO,
        "situacao_valida": _em("situacao", ("campeao", "vice", "rebaixado", "permaneceu")),
    },
    "gold/linha_corte": {
        **TEMPORADA,
        "corte_em_ordem": "pontos_ultimo_salvo >= pontos_primeiro_rebaixado",
        "topo_em_ordem": "pontos_campeao >= pontos_vice",
    },
    "gold/passagens_tecnicos": {
        **TEMPORADA,
        **JOGOS_FECHAM,
        **APROVEITAMENTO,
        "ordem_valida": "ordem >= 1",
        "datas_em_ordem": "data_inicio <= data_fim",
    },
    "gold/esquemas_taticos": {
        "temporada_valida": f"temporada >= {PRIMEIRA_TEMPORADA_ESQUEMAS}",
        **JOGOS_FECHAM,
        **APROVEITAMENTO,
        "uso_pct_valido": "uso_pct BETWEEN 0 AND 1",
    },
    "gold/viradas": {
        "temporada_valida": f"temporada >= {PRIMEIRA_TEMPORADA_GOLS}",
        **RODADA,
        **MANDO,
        "venceu_a_partida": "gols_pro > gols_contra",
        "desvantagem_valida": "maior_desvantagem BETWEEN 1 AND gols_contra",
    },
    "gold/gols_decisivos": {
        "temporada_valida": f"temporada >= {PRIMEIRA_TEMPORADA_GOLS}",
        "contagens_nao_negativas": _nao_negativo(
            "gols_acrescimo", "pontos_ganhos_acrescimo", "pontos_perdidos_acrescimo"
        ),
    },
    "gold/cartoes_jogadores": {
        **TEMPORADA,
        "atleta_nao_nulo": "atleta IS NOT NULL",
        "cartoes_nao_negativos": _nao_negativo("amarelos", "vermelhos"),
    },
    "gold/faltas_clubes": {
        **TEMPORADA,
        "faltas_nao_negativas": _nao_negativo("faltas"),
        "jogos_com_dado_positivos": "jogos_com_dado >= 1",
    },
}


class ConstraintRecusadaError(ValueError):
    """Uma constraint nova não entrou porque o dado que já está na tabela a viola."""


def _referencia_sql(caminho: str) -> str:
    """`delta.<caminho>` para SQL, que só reconhece caminho absoluto (o `./data` padrão não)."""
    absoluto = caminho if "://" in caminho else os.path.abspath(caminho)
    return f"delta.`{absoluto}`"


def constraints_da_tabela(spark: SparkSession, caminho: str) -> dict[str, str]:
    """Nome → expressão das constraints gravadas na tabela."""
    propriedades = DeltaTable.forPath(spark, caminho).detail().first()["properties"]
    return {
        chave.removeprefix(PREFIXO_PROPRIEDADE): valor
        for chave, valor in propriedades.items()
        if chave.startswith(PREFIXO_PROPRIEDADE)
    }


def mesma_expressao(a: str, b: str) -> bool:
    """Iguais a menos de espaços: `x IN ('a', 'b')` volta do Delta como `x IN ( 'a' , 'b' )`."""
    return "".join(a.split()) == "".join(b.split())


def _adicionar(spark: SparkSession, tabela: str, caminho: str, nome: str, expressao: str) -> None:
    try:
        spark.sql(
            f"ALTER TABLE {_referencia_sql(caminho)} ADD CONSTRAINT {nome} CHECK ({expressao})"
        )
    except AnalysisException as erro:
        if erro.getCondition() != "DELTA_NEW_CHECK_CONSTRAINT_VIOLATION":
            raise
        raise ConstraintRecusadaError(
            f"{tabela}: o dado que já está na tabela viola a constraint {nome} ({expressao})"
        ) from erro


def aplicar_constraints(spark: SparkSession, camada: str, nome_tabela: str) -> list[str]:
    """Deixa a tabela com as constraints de `CONSTRAINTS` e devolve as que mudaram.

    Sem nada a mudar, não toca na tabela nem cria versão: é por isso que dá para chamar a cada
    execução. Não dá para usar `DROP CONSTRAINT IF EXISTS` seguido de `ADD`, porque cada um
    grava uma versão, e o `DROP` de constraint que não existe também. Expressão diferente da
    gravada é trocada, depois de conferir o dado, para a tabela não ficar sem a regra antiga
    se a nova for recusada. Constraint que saiu do registro fica na tabela, com um aviso.
    """
    tabela = f"{camada}/{nome_tabela}"
    caminho = config.caminho_tabela(camada, nome_tabela)
    desejadas = CONSTRAINTS[tabela]
    atuais = constraints_da_tabela(spark, caminho)
    mudaram = []
    for nome, expressao in desejadas.items():
        atual = atuais.get(nome)
        if atual is not None and mesma_expressao(atual, expressao):
            continue
        if atual is not None:
            violam = spark.read.format("delta").load(caminho)
            if not violam.where(f"NOT coalesce({expressao}, false)").isEmpty():
                raise ConstraintRecusadaError(
                    f"{tabela}: o dado que já está na tabela viola a constraint {nome} "
                    f"({expressao}); a expressão antiga ({atual}) fica valendo"
                )
            spark.sql(f"ALTER TABLE {_referencia_sql(caminho)} DROP CONSTRAINT {nome}")
        _adicionar(spark, tabela, caminho, nome, expressao)
        log.info("%s: constraint %s aplicada: %s", tabela, nome, expressao)
        mudaram.append(nome)
    for nome in sorted(atuais.keys() - desejadas.keys()):
        log.warning("%s: constraint %s está na tabela e fora do registro", tabela, nome)
    return mudaram


def aplicar_camada(spark: SparkSession, camada: str) -> list[str]:
    """`aplicar_constraints` em toda tabela da camada. Devolve `camada/tabela.constraint` do que
    mudou."""
    mudaram = []
    for tabela in CONSTRAINTS:
        camada_tabela, _, nome = tabela.partition("/")
        if camada_tabela == camada:
            mudaram += [f"{tabela}.{c}" for c in aplicar_constraints(spark, camada, nome)]
    return mudaram
