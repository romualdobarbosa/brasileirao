import pandas as pd
import pytest
from chispa import assert_df_equality

from brasileirao import config, pipeline
from brasileirao.aggregate import exportar, gold


@pytest.fixture(scope="module")
def lake(spark, silver):
    """O gold das fixtures gravado uma vez, com a raiz do lake no ambiente."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("BRASILEIRAO_DATA_ROOT", str(silver))
        gold.processar_gold(spark)
        yield silver


def test_um_parquet_por_tabela_com_o_conteudo_do_gold(spark, lake, tmp_path):
    linhas = exportar.exportar(spark, str(tmp_path))

    # Só os 18 arquivos: nada de pasta temporária, `_SUCCESS` ou `.crc`.
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted(
        f"{nome}.parquet" for nome in gold.TABELAS
    )
    assert set(linhas) == set(gold.TABELAS)
    for nome in gold.TABELAS:
        no_gold = spark.read.format("delta").load(config.caminho_tabela("gold", nome))
        exportado = spark.read.parquet(str(tmp_path / f"{nome}.parquet"))
        assert linhas[nome] == no_gold.count()
        assert_df_equality(
            exportado,
            no_gold,
            ignore_row_order=True,
            ignore_column_order=False,
            ignore_nullable=True,
        )


def test_vem_ordenado_e_arquivo_sem_mudanca_fica_como_esta(spark, lake, tmp_path):
    exportar.exportar(spark, str(tmp_path))
    antes = {p.name: p.stat().st_mtime_ns for p in tmp_path.iterdir()}

    exportar.exportar(spark, str(tmp_path))

    assert {p.name: p.stat().st_mtime_ns for p in tmp_path.iterdir()} == antes
    # Lido sem Spark, como o app lê: já vem ordenado por todas as colunas.
    tabela = pd.read_parquet(tmp_path / "campanhas.parquet")
    ordenada = tabela.sort_values(list(tabela.columns), kind="stable", ignore_index=True)
    pd.testing.assert_frame_equal(tabela, ordenada)


def test_arquivo_estragado_e_trocado(spark, lake, tmp_path):
    exportar.exportar(spark, str(tmp_path))
    antigo = tmp_path / "campanhas.parquet"
    antigo.write_bytes(b"lixo")

    linhas = exportar.exportar(spark, str(tmp_path))

    assert len(pd.read_parquet(antigo)) == linhas["campanhas"] > 0
    assert len(list(tmp_path.iterdir())) == len(gold.TABELAS)


def test_cli_exportar_grava_no_destino(spark, lake, tmp_path):
    pipeline.main(["exportar", "--destino", str(tmp_path / "saida")])

    assert (tmp_path / "saida" / "resumo_temporada.parquet").is_file()


def test_gold_ausente_para_com_a_dica_e_nao_mexe_no_destino(spark, data_root, tmp_path):
    destino = tmp_path / "destino"
    destino.mkdir()
    existente = destino / "goleadas.parquet"
    existente.write_bytes(b"de antes")

    with pytest.raises(SystemExit) as saida:
        pipeline.main(["exportar", "--destino", str(destino)])

    assert "rode `make gold`" in str(saida.value.code)
    assert existente.read_bytes() == b"de antes"
