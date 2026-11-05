"""Conexão psycopg, troca transacional gold_carga → gold e contagens."""

import psycopg
from psycopg import Connection, sql

from brasileirao import config
from brasileirao.serving.tabelas import TABELAS


class PostgresIndisponivelError(RuntimeError):
    pass


def conectar() -> Connection:
    """Conexão em autocommit: a troca abre a própria transação com `conexao.transaction()`."""
    destino = f"{config.pg_host()}:{config.pg_porta()}/{config.pg_banco()}"
    try:
        return psycopg.connect(
            host=config.pg_host(),
            port=config.pg_porta(),
            dbname=config.pg_banco(),
            user=config.pg_usuario(),
            password=config.pg_senha(),
            connect_timeout=5,
            autocommit=True,
        )
    except psycopg.OperationalError as erro:
        raise PostgresIndisponivelError(
            f"Postgres não respondeu em {destino} ({erro}). Localmente, rode `make serving-up`"
        ) from erro


def trocar_gold(conexao: Connection) -> None:
    """Copia gold_carga → gold numa transação só. Se algo falhar, gold fica como estava."""
    with conexao.transaction():
        for tabela in TABELAS:
            alvo = sql.Identifier("gold", tabela.nome)
            origem = sql.Identifier("gold_carga", tabela.nome)
            colunas = sql.SQL(", ").join(map(sql.Identifier, tabela.colunas))
            conexao.execute(sql.SQL("TRUNCATE {}").format(alvo))
            conexao.execute(
                sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                    alvo, colunas, colunas, origem
                )
            )


def contar(conexao: Connection, schema: str) -> dict[str, int]:
    """Linhas de cada tabela de `TABELAS` no schema."""
    return {
        tabela.nome: conexao.execute(
            sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(schema, tabela.nome))
        ).fetchone()[0]
        for tabela in TABELAS
    }
