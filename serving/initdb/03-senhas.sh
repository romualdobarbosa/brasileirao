#!/usr/bin/env bash
# Define as senhas dos papéis a partir do ambiente do container (vindas do .env).
# As variáveis do psql (:'nome') citam o valor, então a senha não vira SQL.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER:-postgres}" --dbname "${POSTGRES_DB:-postgres}" \
    -v senha_carga="$PG_SENHA_CARGA" \
    -v senha_leitor="$PG_SENHA_LEITOR" \
    -v senha_metabase="$PG_SENHA_METABASE" <<'SQL'
ALTER ROLE carga PASSWORD :'senha_carga';
ALTER ROLE leitor PASSWORD :'senha_leitor';
ALTER ROLE metabase PASSWORD :'senha_metabase';
SQL
