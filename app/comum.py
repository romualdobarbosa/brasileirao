"""Leitura, formatos em português, tabelas e gráficos que as páginas repetem."""

import datetime
import unicodedata
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

PASTA_DADOS = Path(__file__).parent / "dados"
REPOSITORIO = "https://github.com/romualdobarbosa/brasileirao"
TODOS_OS_ANOS = "Todos os anos"

AZUL = "#2a78d6"
LARANJA = "#eb6834"
AMARELO = "#eda100"
VERMELHO = "#e34948"
CINZA = "#b5b4ae"

NOTA_BARUERI = (
    "O Barueri de 2009 e o Grêmio Prudente de 2010 são o mesmo clube, "
    "mas aparecem separados, como na fonte."
)


@st.cache_data(show_spinner=False)
def ler(tabela: str) -> pd.DataFrame:
    return pd.read_parquet(PASTA_DADOS / f"{tabela}.parquet")


def nomes_em_ordem(nomes) -> list[str]:
    """Ordem de dicionário: Ávila perto de Avaí, não depois do Z."""

    def chave(nome: str) -> str:
        return unicodedata.normalize("NFKD", nome).encode("ascii", "ignore").decode().casefold()

    return sorted(set(nomes), key=chave)


def pct(valor: float, casas: int = 1) -> str:
    return f"{valor:.{casas}%}".replace(".", ",")


def decimal(valor: float, casas: int = 2) -> str:
    return f"{valor:.{casas}f}".replace(".", ",")


def milhar(valor: int) -> str:
    return f"{valor:,}".replace(",", ".")


def data_br(valor: datetime.date) -> str:
    return valor.strftime("%d/%m/%Y")


def virgula(formato: str) -> str:
    """`labelExpr` de eixo com vírgula decimal: o Vega só formata em inglês."""
    return f"replace(format(datum.value, '{formato}'), '.', ',')"


def filtro_ano(anos, chave: str, com_todos: bool = True, padrao: int | None = None) -> int | None:
    """Selectbox de ano, do mais novo ao mais velho. `None` quer dizer todos os anos."""
    opcoes = sorted(set(anos), reverse=True)
    opcoes_tela = [TODOS_OS_ANOS, *opcoes] if com_todos else opcoes
    indice = opcoes_tela.index(padrao) if padrao is not None else 0
    escolha = st.selectbox("Ano", opcoes_tela, index=indice, key=chave)
    return None if escolha == TODOS_OS_ANOS else escolha


def tabela(
    df: pd.DataFrame,
    *,
    pcts=(),
    decimais=(),
    milhares=(),
    datas=(),
    zero_vazio=(),
    fixar=(),
    max_linhas: int = 25,
) -> None:
    """Tabela em português. Os valores seguem numéricos: clicar no título ordena certo."""
    formatos = {c: pct for c in pcts}
    formatos.update({c: decimal for c in decimais})
    formatos.update({c: milhar for c in milhares})
    formatos.update({c: data_br for c in datas})
    formatos.update({c: (lambda v: str(v) if v else "") for c in zero_vazio})
    linhas = min(len(df), max_linhas)
    st.dataframe(
        df.reset_index(drop=True).style.format(formatos, na_rep=""),
        hide_index=True,
        height=(linhas + 1) * 35 + 3,
        column_config={c: st.column_config.Column(pinned=True) for c in fixar},
    )


def desenhar(grafico: alt.TopLevelMixin, descricao: str) -> None:
    st.altair_chart(grafico, width="stretch", alt=descricao)


def barras(
    df: pd.DataFrame,
    categoria: str,
    valor: str,
    rotulo: str | None = None,
    dicas: list | None = None,
) -> alt.LayerChart:
    """Barras horizontais na ordem do `df`, com o número na ponta. `rotulo` é uma coluna de
    texto já formatado; sem ele, vai o próprio `valor`."""
    base = alt.Chart(df).encode(
        y=alt.Y(f"{categoria}:N", sort=None, title=None, axis=alt.Axis(labelLimit=170, ticks=False))
    )
    dicas = dicas or [alt.Tooltip(f"{categoria}:N", title=categoria), rotulo or valor]
    corpo = base.mark_bar(color=AZUL, size=18, cornerRadiusEnd=4).encode(
        x=alt.X(f"{valor}:Q", title=None, axis=None), tooltip=dicas
    )
    numero = base.mark_text(align="left", dx=4).encode(
        x=alt.X(f"{valor}:Q"), text=f"{rotulo or valor}:{'N' if rotulo else 'Q'}", tooltip=dicas
    )
    return (corpo + numero).properties(height=max(26 * len(df), 80))


def rodape() -> None:
    st.divider()
    st.caption(
        "Os números saem do pipeline em Spark que eu montei em cima de uma base pública do "
        "Brasileirão. Este app só lê o resultado, sem banco e sem IA. "
        f"O código está em [github.com/romualdobarbosa/brasileirao]({REPOSITORIO})."
    )
