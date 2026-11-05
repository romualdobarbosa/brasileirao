-- Papéis do serving, sem senha (quem define é o 03-senhas.sh), e o banco do Metabase.
-- Roda no banco de dados (brasileirao no compose). Os papéis são do cluster inteiro,
-- então este arquivo aguenta ser aplicado de novo num cluster onde eles já existem.

DO $$
DECLARE
    papel text;
BEGIN
    FOREACH papel IN ARRAY ARRAY['carga', 'leitor', 'metabase'] LOOP
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = papel) THEN
            EXECUTE format('CREATE ROLE %I LOGIN', papel);
        END IF;
    END LOOP;
END
$$;

-- Só carga e leitor conectam no banco de dados; o metabase não o enxerga.
DO $$
BEGIN
    EXECUTE format('REVOKE ALL ON DATABASE %I FROM PUBLIC', current_database());
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO carga, leitor', current_database());
END
$$;

-- CREATE DATABASE não roda em bloco DO nem em transação, e falha se o banco já existe.
-- Por isso fica isolado depois desta marca: quem reaplica o arquivo num cluster que já
-- tem o banco metabase (a fixture dos testes) roda só o que vem antes dela.
-- metabase:inicio
CREATE DATABASE metabase OWNER metabase;
REVOKE ALL ON DATABASE metabase FROM PUBLIC;
