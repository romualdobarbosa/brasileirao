"""Baixa o dataset do Kaggle, na versão fixada, para `<data_root>/raw/`."""

import logging
import shutil
from pathlib import Path

import kagglehub
from kagglehub.exceptions import CredentialError, KaggleApiHTTPError, UnauthenticatedError

from brasileirao import config

log = logging.getLogger(__name__)

# Trocar de versão é decisão explícita (ver "Contrato de dados" na spec).
DATASET = "adaoduque/campeonato-brasileiro-de-futebol/versions/18"
ARQUIVOS = (
    "campeonato-brasileiro-full.csv",
    "campeonato-brasileiro-gols.csv",
    "campeonato-brasileiro-cartoes.csv",
    "campeonato-brasileiro-estatisticas-full.csv",
    "Legenda.txt",
)
MENSAGEM_CREDENCIAL = (
    "O Kaggle recusou a credencial. Gere um token em https://www.kaggle.com/settings "
    "e salve em ~/.kaggle/access_token (ou exporte KAGGLE_API_TOKEN)."
)


class CredencialKaggleError(RuntimeError):
    pass


def baixar_dataset() -> Path:
    """Baixa para o cache do kagglehub e devolve o diretório com os arquivos."""
    try:
        return Path(kagglehub.dataset_download(DATASET))
    except (CredentialError, UnauthenticatedError) as erro:
        raise CredencialKaggleError(MENSAGEM_CREDENCIAL) from erro
    except KaggleApiHTTPError as erro:
        if erro.response is not None and erro.response.status_code in (401, 403):
            raise CredencialKaggleError(MENSAGEM_CREDENCIAL) from erro
        raise


def copiar_para_raw(origem: Path, destino: Path) -> list[Path]:
    """Copia os arquivos do dataset para `destino`, sobrescrevendo o que já existir."""
    faltando = [nome for nome in ARQUIVOS if not (origem / nome).is_file()]
    if faltando:
        raise FileNotFoundError(f"Arquivos ausentes no download em {origem}: {', '.join(faltando)}")
    destino.mkdir(parents=True, exist_ok=True)
    return [Path(shutil.copy2(origem / nome, destino / nome)) for nome in ARQUIVOS]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    try:
        origem = baixar_dataset()
    except CredencialKaggleError as erro:
        raise SystemExit(str(erro)) from erro
    copiados = copiar_para_raw(origem, Path(config.caminho_camada("raw")))
    log.info("%d arquivos de %s copiados para %s", len(copiados), DATASET, copiados[0].parent)


if __name__ == "__main__":
    main()
