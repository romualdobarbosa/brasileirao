"""Alerta de falha da DAG `brasileirao_rodada` por e-mail, pelo SMTP do Gmail.

O `on_failure_callback` das tarefas (orchestration/dags/) chama `ao_falhar`. A configuração vem
do ambiente do container, que o compose lê do `orchestration/.env` (fora do git):

- `ALERTA_SMTP_HOST` (ex.: smtp.gmail.com) e `ALERTA_SMTP_PORTA` (587 com STARTTLS, padrão,
  ou 465 com SSL);
- `ALERTA_SMTP_USUARIO` e `ALERTA_SMTP_SENHA` (no Gmail, a conta e uma senha de app);
- `ALERTA_EMAIL_PARA` (destinatário) e, opcional, `ALERTA_EMAIL_DE` (padrão: o usuário).

Sem host, usuário, senha ou destinatário, o alerta fica desligado e só aparece no log. Nada
aqui levanta exceção para quem chama: um envio que falha vira um aviso no log da tarefa.

Sem importar o Airflow, para ter teste na suíte normal.
"""

import logging
import os
import smtplib
import ssl
from collections.abc import Mapping
from dataclasses import dataclass
from email.message import EmailMessage

log = logging.getLogger(__name__)

PORTA_PADRAO = 587
PORTA_SSL = 465
OBRIGATORIAS = (
    "ALERTA_SMTP_HOST",
    "ALERTA_SMTP_USUARIO",
    "ALERTA_SMTP_SENHA",
    "ALERTA_EMAIL_PARA",
)
TIMEOUT = 30


@dataclass(frozen=True)
class ConfigSmtp:
    host: str
    porta: int
    usuario: str
    senha: str
    para: str
    de: str

    def __repr__(self) -> str:
        # A senha nunca vai para o log, nem por engano.
        return f"ConfigSmtp(host={self.host!r}, porta={self.porta}, usuario={self.usuario!r})"


@dataclass(frozen=True)
class Falha:
    dag_id: str
    run_id: str
    tarefa: str
    rodada: object
    alvo: object
    erro: str
    log_url: str | None = None


def config_do_ambiente(ambiente: Mapping[str, str] | None = None) -> ConfigSmtp | None:
    """A configuração do SMTP, ou `None` (alerta desligado) se faltar alguma obrigatória."""
    ambiente = os.environ if ambiente is None else ambiente
    faltando = [v for v in OBRIGATORIAS if not ambiente.get(v, "").strip()]
    if faltando:
        log.info("alerta desligado: faltam %s no ambiente", ", ".join(faltando))
        return None
    porta = ambiente.get("ALERTA_SMTP_PORTA", "").strip() or str(PORTA_PADRAO)
    if not porta.isdigit():
        log.warning("alerta desligado: ALERTA_SMTP_PORTA precisa ser um número, não %r", porta)
        return None
    usuario = ambiente["ALERTA_SMTP_USUARIO"].strip()
    return ConfigSmtp(
        host=ambiente["ALERTA_SMTP_HOST"].strip(),
        porta=int(porta),
        usuario=usuario,
        senha=ambiente["ALERTA_SMTP_SENHA"],
        para=ambiente["ALERTA_EMAIL_PARA"].strip(),
        de=ambiente.get("ALERTA_EMAIL_DE", "").strip() or usuario,
    )


def montar_mensagem(falha: Falha, config: ConfigSmtp) -> EmailMessage:
    mensagem = EmailMessage()
    mensagem["Subject"] = (
        f"[brasileirao] falha em {falha.dag_id}: {falha.tarefa} (rodada {falha.rodada})"
    )
    mensagem["From"] = config.de
    mensagem["To"] = config.para
    linhas = [
        f"A tarefa {falha.tarefa} da DAG {falha.dag_id} falhou.",
        "",
        f"dag_id: {falha.dag_id}",
        f"run_id: {falha.run_id}",
        f"tarefa: {falha.tarefa}",
        f"rodada: {falha.rodada}",
        f"alvo: {falha.alvo}",
        f"erro: {falha.erro}",
    ]
    if falha.log_url:
        linhas.append(f"log: {falha.log_url}")
    linhas += ["", "As tarefas seguintes ficam em upstream_failed. Simulação de 2025."]
    mensagem.set_content("\n".join(linhas))
    return mensagem


def enviar(mensagem: EmailMessage, config: ConfigSmtp) -> None:
    """Envia pelo SMTP: SSL direto na 465, STARTTLS nas outras. Levanta se falhar."""
    contexto = ssl.create_default_context()
    if config.porta == PORTA_SSL:
        conexao = smtplib.SMTP_SSL(config.host, config.porta, timeout=TIMEOUT, context=contexto)
    else:
        conexao = smtplib.SMTP(config.host, config.porta, timeout=TIMEOUT)
    with conexao as smtp:
        if config.porta != PORTA_SSL:
            smtp.starttls(context=contexto)
        smtp.login(config.usuario, config.senha)
        smtp.send_message(mensagem)


def alertar(falha: Falha, ambiente: Mapping[str, str] | None = None) -> bool:
    """Manda o e-mail da falha. Devolve se mandou; desligado ou com erro, só loga."""
    config = config_do_ambiente(ambiente)
    if config is None:
        log.info("alerta desligado: falha de %s em %s não enviada", falha.tarefa, falha.run_id)
        return False
    try:
        enviar(montar_mensagem(falha, config), config)
    except Exception as erro:  # o alerta nunca derruba nada
        log.warning(
            "alerta não enviado para %s via %s:%d (%s: %s)",
            config.para,
            config.host,
            config.porta,
            type(erro).__name__,
            erro,
        )
        return False
    log.info("alerta enviado para %s: falha de %s em %s", config.para, falha.tarefa, falha.run_id)
    return True


def falha_do_contexto(contexto: Mapping) -> Falha:
    """Os dados da falha a partir do contexto que o Airflow passa ao callback."""
    ti = contexto.get("ti") or contexto.get("task_instance")
    dag_run = contexto.get("dag_run")
    params = contexto.get("params") or {}
    try:
        log_url = getattr(ti, "log_url", None)
    except Exception:  # a URL depende da configuração do Airflow; sem ela, segue sem link
        log_url = None
    return Falha(
        dag_id=getattr(ti, "dag_id", None) or getattr(dag_run, "dag_id", "?"),
        run_id=getattr(dag_run, "run_id", None) or getattr(ti, "run_id", "?"),
        tarefa=getattr(ti, "task_id", "?"),
        rodada=params.get("rodada"),
        alvo=params.get("alvo"),
        erro=str(contexto.get("exception") or contexto.get("reason") or "sem detalhe"),
        log_url=log_url,
    )


def ao_falhar(contexto: Mapping) -> None:
    """O `on_failure_callback` das tarefas: monta a falha e alerta, sem nunca levantar."""
    try:
        alertar(falha_do_contexto(contexto))
    except Exception:
        log.exception("alerta: erro inesperado no callback de falha")
