from brasileirao.aggregate import gold
from brasileirao.serving.tabelas import DRIVER_JDBC, TABELAS


def test_as_tabelas_do_serving_sao_as_primeiras_do_registro_do_gold_na_mesma_ordem():
    """As tabelas da vitrine vêm depois no registro e não vão para o Postgres."""
    assert [t.nome for t in TABELAS] == list(gold.TABELAS)[: len(TABELAS)]


def test_pk_e_subconjunto_das_colunas_e_colunas_sem_repeticao():
    for tabela in TABELAS:
        assert len(set(tabela.colunas)) == len(tabela.colunas), tabela.nome
        assert set(tabela.pk) <= set(tabela.colunas), tabela.nome


def test_driver_jdbc_com_versao_exata():
    grupo, artefato, versao = DRIVER_JDBC.split(":")
    assert (grupo, artefato) == ("org.postgresql", "postgresql")
    assert versao.startswith("42.7.") and versao.split(".")[-1].isdigit()
