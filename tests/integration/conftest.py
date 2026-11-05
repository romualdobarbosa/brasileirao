import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from brasileirao import pipeline

FIXTURES_RAW_GOLD = Path(__file__).parent.parent / "fixtures" / "raw_gold"


@pytest.fixture(scope="module")
def silver(spark, tmp_path_factory):
    """Silver das fixtures `raw_gold` (o minicampeonato em 2019), montada uma vez por módulo."""
    raiz = tmp_path_factory.mktemp("lake")
    shutil.copytree(FIXTURES_RAW_GOLD, raiz / "raw")
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("BRASILEIRAO_DATA_ROOT", str(raiz))
        # Com 4 times, a regra real de 2019 rebaixaria todo mundo, inclusive o campeão.
        for modulo in ("campanhas", "resumo"):
            mp.setattr(f"brasileirao.aggregate.{modulo}.rebaixados_por_temporada", lambda _: 1)
        assert pipeline.lakehouse(spark, None) == [2019]
        yield raiz


INITDB = Path(__file__).parent.parent.parent / "serving" / "initdb"
MARCA_METABASE = "-- metabase:inicio"
PAPEIS_COM_SENHA = ("carga", "leitor")


@dataclass(frozen=True)
class PostgresTeste:
    url: str
    banco: str
    senhas: dict[str, str]

    def conectar(self, papel: str | None = None) -> psycopg.Connection:
        """Superusuário no banco descartável ou, com `papel`, login como ele."""
        if papel is None:
            return psycopg.connect(self.url, dbname=self.banco, autocommit=True)
        return psycopg.connect(
            self.url, dbname=self.banco, user=papel, password=self.senhas[papel], autocommit=True
        )


def _aplicar_initdb(conexao: psycopg.Connection) -> None:
    """Os mesmos `.sql` do container. Os papéis são do cluster e podem já existir (compose
    local); o banco metabase também, então o trecho depois da marca roda à parte, uma
    instrução por vez (CREATE DATABASE não roda em transação) e só se ele faltar."""
    papeis, banco_metabase = (INITDB / "01-papeis.sql").read_text().split(MARCA_METABASE)
    conexao.execute(papeis)
    if not conexao.execute("SELECT 1 FROM pg_database WHERE datname = 'metabase'").fetchone():
        for instrucao in banco_metabase.split(";"):
            if instrucao.strip():
                conexao.execute(instrucao)
    conexao.execute((INITDB / "02-gold.sql").read_text())


@pytest.fixture(scope="module")
def postgres_teste():
    """Banco descartável num Postgres real, com o `initdb/` do projeto aplicado.

    Precisa de `BRASILEIRAO_PG_TESTE_URL` (superusuário) e de `PG_SENHA_CARGA` e
    `PG_SENHA_LEITOR`, as mesmas senhas que o compose usa: os papéis são do cluster, então a
    fixture define a senha deles com esses valores, sem mudar nada num cluster já de pé.
    """
    url = os.environ.get("BRASILEIRAO_PG_TESTE_URL")
    if not url:
        pytest.skip("BRASILEIRAO_PG_TESTE_URL não definida (veja serving/.env.example)")
    senhas = {papel: os.environ.get(f"PG_SENHA_{papel.upper()}") for papel in PAPEIS_COM_SENHA}
    faltando = [f"PG_SENHA_{papel.upper()}" for papel, senha in senhas.items() if not senha]
    if faltando:
        pytest.skip(f"{', '.join(faltando)} não definida(s) (veja serving/.env.example)")
    banco = f"brasileirao_teste_{uuid.uuid4().hex[:8]}"
    try:
        admin = psycopg.connect(url, autocommit=True, connect_timeout=5)
    except psycopg.OperationalError:
        admin = None
    if admin is None:
        # Fora do except, sem traceback e sem a URL (que tem a senha): só onde e o que fazer.
        info = conninfo_to_dict(url)
        destino = f"{info.get('host', 'localhost')}:{info.get('port', 5432)}"
        pytest.fail(
            f"Postgres de teste em {destino} não respondeu. Suba com `make serving-up` ou "
            "rode `make test BRASILEIRAO_PG_TESTE_URL=` para pular os testes postgres",
            pytrace=False,
        )
    with admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(banco)))
    try:
        pg = PostgresTeste(url, banco, senhas)
        with pg.conectar() as conexao:
            _aplicar_initdb(conexao)
            for papel, senha in senhas.items():
                conexao.execute(
                    sql.SQL("ALTER ROLE {} PASSWORD {}").format(
                        sql.Identifier(papel), sql.Literal(senha)
                    )
                )
        yield pg
    finally:
        with psycopg.connect(url, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(banco))
            )
