from brasileirao.aggregate import gold
from brasileirao.serving.tabelas import DRIVER_JDBC, TABELAS


def test_nomes_batem_com_o_registro_do_gold_na_mesma_ordem():
    assert [t.nome for t in TABELAS] == list(gold.TABELAS)


def test_pk_e_subconjunto_das_colunas_e_colunas_sem_repeticao():
    for tabela in TABELAS:
        assert len(set(tabela.colunas)) == len(tabela.colunas), tabela.nome
        assert set(tabela.pk) <= set(tabela.colunas), tabela.nome


def test_driver_jdbc_com_versao_exata():
    grupo, artefato, versao = DRIVER_JDBC.split(":")
    assert (grupo, artefato) == ("org.postgresql", "postgresql")
    assert versao.startswith("42.7.") and versao.split(".")[-1].isdigit()
