from chispa import assert_df_equality

from brasileirao import config


def test_escreve_e_le_tabela_delta(spark, data_root):
    esperado = spark.createDataFrame(
        [(1, "Flamengo", 2003), (2, "Gremio", 2003)],
        "partida_id int, mandante string, temporada int",
    )
    caminho = config.caminho_tabela("bronze", "teste")

    esperado.write.format("delta").mode("overwrite").partitionBy("temporada").save(caminho)
    lido = spark.read.format("delta").load(caminho)

    assert caminho.startswith(str(data_root))
    assert (data_root / "bronze" / "teste" / "_delta_log").is_dir()
    assert_df_equality(lido, esperado, ignore_row_order=True)
