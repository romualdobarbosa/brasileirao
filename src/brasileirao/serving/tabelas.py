"""As 8 tabelas gold do lado do Postgres: nome, colunas em ordem e chave primária.

É a única fonte de identificador de tabela e coluna para o SQL da carga. Precisa bater com
o registro de `aggregate/gold.py`, com o schema que o gold grava e com o DDL de
`serving/initdb/02-gold.sql` (os testes conferem os três).
"""

from dataclasses import dataclass

# Conferida no Maven Central em 28/10/2026.
DRIVER_JDBC = "org.postgresql:postgresql:42.7.14"


@dataclass(frozen=True)
class Tabela:
    nome: str
    colunas: tuple[str, ...]
    pk: tuple[str, ...]


TABELAS: tuple[Tabela, ...] = (
    Tabela(
        "classificacao_por_rodada",
        (
            "temporada",
            "rodada",
            "clube",
            "posicao",
            "pontos",
            "ajuste_pontos",
            "jogos",
            "vitorias",
            "empates",
            "derrotas",
            "gols_pro",
            "gols_contra",
            "saldo",
            "aproveitamento",
            "ultimos_5",
        ),
        ("temporada", "rodada", "clube"),
    ),
    Tabela(
        "desempenho_mandante_visitante",
        (
            "temporada",
            "clube",
            "mando",
            "jogos",
            "vitorias",
            "empates",
            "derrotas",
            "gols_pro",
            "gols_contra",
            "pontos",
            "aproveitamento",
        ),
        ("temporada", "clube", "mando"),
    ),
    Tabela(
        "sequencias",
        ("temporada", "clube", "tipo", "jogos", "data_inicio", "data_fim", "em_aberto"),
        ("temporada", "clube", "tipo"),
    ),
    Tabela(
        "ranking_historico",
        (
            "posicao",
            "clube",
            "temporadas",
            "primeira_temporada",
            "ultima_temporada",
            "jogos",
            "vitorias",
            "empates",
            "derrotas",
            "gols_pro",
            "gols_contra",
            "saldo",
            "pontos",
            "aproveitamento",
            "titulos",
        ),
        ("clube",),
    ),
    Tabela(
        "disciplina",
        (
            "temporada",
            "clube",
            "jogos",
            "amarelos",
            "vermelhos",
            "amarelos_por_jogo",
            "vermelhos_por_jogo",
            "fonte",
        ),
        ("temporada", "clube"),
    ),
    Tabela(
        "artilharia",
        ("temporada", "posicao", "atleta", "clubes", "gols", "penaltis"),
        ("temporada", "atleta"),
    ),
    Tabela(
        "gols_por_minuto",
        ("temporada", "faixa", "lado", "gols", "pct_gols"),
        ("temporada", "faixa", "lado"),
    ),
    Tabela(
        "resumo_temporada",
        (
            "temporada",
            "times",
            "partidas",
            "gols",
            "media_gols",
            "pct_vitoria_mandante",
            "pct_empate",
            "pct_vitoria_visitante",
            "campeao",
            "vice",
            "rebaixados",
            "artilheiro",
            "gols_artilheiro",
        ),
        ("temporada",),
    ),
)
