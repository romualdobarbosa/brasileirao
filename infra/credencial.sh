#!/bin/sh
# Imprime a credencial da sessão do `aws login` como `export ...`, para `eval "$(infra/credencial.sh)"`.
#
# A credencial que o `aws configure export-credentials` entrega dura 15 min, e a CLI só troca por
# uma nova quando faltam 5 min. O JVM do Spark lê a credencial do ambiente uma vez, na partida, e
# não renova. Por isso este script espera a credencial ter pelo menos MINUTOS de validade
# (padrão 6) antes de entregar, e cada processo do Spark tem de terminar dentro desse prazo.
set -eu
# Uma credencial já exportada no ambiente teria precedência sobre a sessão do `aws login` e
# nunca seria renovada.
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_CREDENTIAL_EXPIRATION
minimo=$(( ${1:-6} * 60 ))
limite=$(( $(date +%s) + 900 ))
while :; do
    expira=$(aws configure export-credentials --format process |
        python3 -c "import json, sys; print(json.load(sys.stdin)['Expiration'])")
    restante=$(( $(date -d "$expira" +%s) - $(date +%s) ))
    [ "$restante" -ge "$minimo" ] && break
    [ "$(date +%s)" -lt "$limite" ] || { echo "credencial não renovou em 15 min" >&2; exit 1; }
    echo "credencial com ${restante}s de validade; esperando a renovação" >&2
    sleep 20
done
echo "credencial válida por ${restante}s" >&2
aws configure export-credentials --format env
