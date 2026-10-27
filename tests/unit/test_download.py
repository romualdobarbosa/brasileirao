import pytest
import requests
from kagglehub.exceptions import KaggleApiHTTPError, UnauthenticatedError

from brasileirao.ingestion import download


@pytest.fixture
def cache_kaggle(tmp_path):
    """Diretório no formato do que o kagglehub devolve, com os 5 arquivos."""
    origem = tmp_path / "cache"
    origem.mkdir()
    for nome in download.ARQUIVOS:
        (origem / nome).write_text(f"conteudo de {nome}", encoding="utf-8")
    return origem


def _http_error(status: int) -> KaggleApiHTTPError:
    resposta = requests.Response()
    resposta.status_code = status
    return KaggleApiHTTPError("erro", response=resposta)


def test_copia_os_arquivos_para_raw(cache_kaggle, tmp_path):
    destino = tmp_path / "data" / "raw"

    copiados = download.copiar_para_raw(cache_kaggle, destino)

    assert sorted(p.name for p in destino.iterdir()) == sorted(download.ARQUIVOS)
    assert len(copiados) == 5
    assert (destino / "Legenda.txt").read_text(encoding="utf-8") == "conteudo de Legenda.txt"


def test_rodar_de_novo_sobrescreve(cache_kaggle, tmp_path):
    destino = tmp_path / "raw"
    download.copiar_para_raw(cache_kaggle, destino)
    (cache_kaggle / "campeonato-brasileiro-full.csv").write_text("novo", encoding="utf-8")

    download.copiar_para_raw(cache_kaggle, destino)

    assert (destino / "campeonato-brasileiro-full.csv").read_text(encoding="utf-8") == "novo"


def test_arquivo_ausente_no_download(cache_kaggle, tmp_path):
    (cache_kaggle / "campeonato-brasileiro-gols.csv").unlink()

    with pytest.raises(FileNotFoundError, match="campeonato-brasileiro-gols.csv"):
        download.copiar_para_raw(cache_kaggle, tmp_path / "raw")


def test_baixa_a_versao_fixada(monkeypatch, cache_kaggle):
    pedidos = []
    monkeypatch.setattr(
        download.kagglehub,
        "dataset_download",
        lambda handle: pedidos.append(handle) or str(cache_kaggle),
    )

    assert download.baixar_dataset() == cache_kaggle
    assert pedidos == ["adaoduque/campeonato-brasileiro-de-futebol/versions/18"]


@pytest.mark.parametrize("erro", [UnauthenticatedError("sem login"), _http_error(401)])
def test_credencial_recusada_diz_onde_por_o_token(monkeypatch, erro):
    def falha(handle):
        raise erro

    monkeypatch.setattr(download.kagglehub, "dataset_download", falha)

    with pytest.raises(download.CredencialKaggleError, match=r"~/\.kaggle/access_token"):
        download.baixar_dataset()


def test_outro_erro_http_passa_adiante(monkeypatch):
    def falha(handle):
        raise _http_error(500)

    monkeypatch.setattr(download.kagglehub, "dataset_download", falha)

    with pytest.raises(KaggleApiHTTPError):
        download.baixar_dataset()


def test_main_copia_para_o_data_root(monkeypatch, data_root, cache_kaggle):
    monkeypatch.setattr(download.kagglehub, "dataset_download", lambda handle: str(cache_kaggle))

    download.main()

    assert sorted(p.name for p in (data_root / "raw").iterdir()) == sorted(download.ARQUIVOS)


def test_main_sem_credencial_sai_com_mensagem(monkeypatch, data_root):
    def falha(handle):
        raise UnauthenticatedError("sem login")

    monkeypatch.setattr(download.kagglehub, "dataset_download", falha)

    with pytest.raises(SystemExit, match="access_token"):
        download.main()
