"""Operações sobre as tabelas Delta do lake: constraints, manutenção e histórico."""

from brasileirao import config


class TabelaInvalidaError(ValueError):
    pass


def caminho_de(tabela: str) -> str:
    """`camada/tabela` (ex.: `silver/partidas`) → caminho da tabela no lake."""
    camada, separador, nome = tabela.partition("/")
    if not separador or not nome or camada not in config.CAMADAS:
        raise TabelaInvalidaError(
            f"Tabela {tabela!r} inválida: use camada/tabela, como silver/partidas"
        )
    return config.caminho_tabela(camada, nome)
