import shutil

import psycopg
import pytest
from psycopg import errors, sql
from psycopg.conninfo import conninfo_to_dict

from brasileirao import pipeline
from brasileirao.aggregate import gold
from brasileirao.serving import carga, postgres
from brasileirao.serving.tabelas import TABELAS

TIPOS_POSTGRES = {"int": "integer", "double": "double precision", "string": "text"}


@pytest.fixture(scope="module")
def lake_gold(postgres_teste, spark, silver):
    """O gold das fixtures `raw_gold`. Pede o Postgres primeiro: sem ele, pula antes de
    montar o lake."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("BRASILEIRAO_DATA_ROOT", str(silver))
        shutil.rmtree(silver / "gold", ignore_errors=True)
        linhas = gold.processar_gold(spark)
    return silver, linhas


@pytest.fixture
def pg(postgres_teste, lake_gold, monkeypatch):
    """Ambiente da carga apontando pro banco descartável, como o papel carga."""
    raiz, _ = lake_gold
    info = conninfo_to_dict(postgres_teste.url)
    monkeypatch.setenv("BRASILEIRAO_DATA_ROOT", str(raiz))
    monkeypatch.setenv("BRASILEIRAO_PG_HOST", info.get("host", "localhost"))
    monkeypatch.setenv("BRASILEIRAO_PG_PORT", str(info.get("port", 5432)))
    monkeypatch.setenv("BRASILEIRAO_PG_DB", postgres_teste.banco)
    monkeypatch.setenv("BRASILEIRAO_PG_USUARIO", "carga")
    monkeypatch.setenv("BRASILEIRAO_PG_SENHA", postgres_teste.senhas["carga"])
    return postgres_teste


def _conteudo(conexao):
    """Todas as linhas de `gold`, ordenadas pela PK de cada tabela."""
    return {
        t.nome: conexao.execute(
            sql.SQL("SELECT * FROM {} ORDER BY {}").format(
                sql.Identifier("gold", t.nome), sql.SQL(", ").join(map(sql.Identifier, t.pk))
            )
        ).fetchall()
        for t in TABELAS
    }


def _oids(conexao):
    return dict(
        conexao.execute(
            "SELECT relname, oid FROM pg_class WHERE relnamespace = 'gold'::regnamespace "
            "AND relkind = 'r'"
        ).fetchall()
    )


@pytest.mark.postgres
def test_carga_deixa_as_contagens_do_delta(spark, pg, lake_gold):
    _, linhas = lake_gold

    assert carga.carregar(spark) == linhas

    with pg.conectar() as conexao:
        final = conexao.execute(
            "SELECT clube, pontos, ultimos_5 FROM gold.classificacao_por_rodada "
            "WHERE rodada = 6 ORDER BY posicao"
        ).fetchall()
        resumo = conexao.execute(
            "SELECT campeao, artilheiro, gols_artilheiro, media_gols FROM gold.resumo_temporada"
        ).fetchone()
        invencibilidade = conexao.execute(
            "SELECT jogos, data_inicio, data_fim FROM gold.sequencias "
            "WHERE clube = 'Flamengo' AND tipo = 'invencibilidade'"
        ).fetchone()
    assert [(clube, pontos) for clube, pontos, _ in final] == [
        ("Flamengo", 14),
        ("Palmeiras", 7),
        ("Santos", 7),
        ("São Paulo", 3),
    ]
    assert resumo[:3] == ("Flamengo", "Arrascaeta, Gabriel", 1)
    assert isinstance(resumo[3], float)
    # date do Delta chega como date, não como texto.
    assert invencibilidade[0] > 0 and invencibilidade[1] <= invencibilidade[2]


@pytest.mark.postgres
def test_carga_duas_vezes_e_igual_a_uma_e_mantem_as_tabelas(spark, pg):
    carga.carregar(spark)
    with pg.conectar() as conexao:
        uma_vez, oids = _conteudo(conexao), _oids(conexao)

    carga.carregar(spark)

    with pg.conectar() as conexao:
        assert _conteudo(conexao) == uma_vez
        # TRUNCATE + INSERT: as tabelas não são recriadas, e o Metabase não perde a referência.
        assert _oids(conexao) == oids
    assert len(oids) == len(TABELAS)


@pytest.mark.postgres
def test_falha_no_meio_da_troca_nao_muda_gold(spark, pg):
    carga.carregar(spark)
    with pg.conectar() as conexao:
        antes = _conteudo(conexao)
        conexao.execute("TRUNCATE gold_carga.classificacao_por_rodada")
        # A última tabela da troca: as anteriores já foram truncadas quando ela falha.
        conexao.execute("ALTER TABLE gold_carga.resumo_temporada DROP COLUMN vice")

        with pytest.raises(errors.UndefinedColumn, match="vice"):
            postgres.trocar_gold(conexao)

        assert _conteudo(conexao) == antes


@pytest.mark.postgres
def test_contagem_diferente_e_falha(spark, pg, monkeypatch):
    contar = postgres.contar

    def contar_errado(conexao, schema):
        return {**contar(conexao, schema), "artilharia": 0}

    monkeypatch.setattr(postgres, "contar", contar_errado)

    with pytest.raises(carga.CargaError, match="artilharia: Delta 2, Postgres 0"):
        carga.carregar(spark)


@pytest.mark.postgres
def test_ddl_bate_com_tabelas_e_com_os_tipos_do_gold(spark, pg):
    with pg.conectar() as conexao:
        colunas = conexao.execute(
            "SELECT table_name, column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'gold' ORDER BY table_name, ordinal_position"
        ).fetchall()
        pks = conexao.execute(
            "SELECT k.table_name, k.column_name FROM information_schema.table_constraints c "
            "JOIN information_schema.key_column_usage k USING (constraint_schema, "
            "constraint_name) WHERE c.table_schema = 'gold' AND c.constraint_type = "
            "'PRIMARY KEY' ORDER BY k.table_name, k.ordinal_position"
        ).fetchall()
    ddl = {}
    for tabela, coluna, tipo in colunas:
        ddl.setdefault(tabela, []).append((coluna, tipo))
    pk = {}
    for tabela, coluna in pks:
        pk.setdefault(tabela, []).append(coluna)

    assert {t: [c for c, _ in cols] for t, cols in ddl.items()} == {
        t.nome: list(t.colunas) for t in TABELAS
    }
    assert pk == {t.nome: list(t.pk) for t in TABELAS}
    for nome, df in carga.ler_gold(spark).items():
        esperado = [
            (f.name, TIPOS_POSTGRES.get(f.dataType.simpleString(), f.dataType.simpleString()))
            for f in df.schema.fields
        ]
        assert ddl[nome] == esperado, nome


@pytest.mark.postgres
def test_leitor_so_le_gold(pg):
    with pg.conectar("leitor") as leitor:
        assert leitor.execute("SELECT count(*) FROM gold.resumo_temporada").fetchone()[0] >= 0
        with pytest.raises(errors.InsufficientPrivilege):
            leitor.execute("INSERT INTO gold.resumo_temporada (temporada) VALUES (1900)")
        with pytest.raises(errors.InsufficientPrivilege):
            leitor.execute("CREATE TABLE gold.x (a int)")
        with pytest.raises(errors.InsufficientPrivilege):
            leitor.execute("SELECT * FROM gold_carga.resumo_temporada")
        with pytest.raises(errors.InsufficientPrivilege):
            leitor.execute("CREATE TABLE public.x (a int)")
        with pytest.raises(errors.InsufficientPrivilege):
            leitor.execute("CREATE TEMP TABLE x (a int)")

    with pytest.raises(psycopg.OperationalError, match="permission denied for database"):
        psycopg.connect(
            pg.url, dbname="metabase", user="leitor", password=pg.senhas["leitor"]
        ).close()


@pytest.mark.postgres
def test_cli_carga(pg):
    pipeline.main(["carga"])

    with pg.conectar() as conexao:
        assert conexao.execute("SELECT count(*) FROM gold.ranking_historico").fetchone()[0] == 4


def test_sem_gold_manda_rodar_make_gold(spark, data_root):
    with pytest.raises(carga.GoldAusenteError, match="make gold"):
        carga.carregar(spark)


def test_cli_carga_sem_gold(spark, data_root):
    with pytest.raises(SystemExit) as saida:
        pipeline.main(["carga"])

    assert "make gold" in str(saida.value.code)


def test_gold_com_schema_diferente_falha_antes_de_gravar(spark, data_root):
    caminho = data_root / "gold" / TABELAS[0].nome
    spark.createDataFrame([(2019, 1)], "temporada int, rodada int").write.format("delta").save(
        str(caminho)
    )

    with pytest.raises(carga.CargaError, match="mudou de schema"):
        carga.carregar(spark)
