# Airflow com Java e o projeto, para a DAG brasileirao_rodada. Build a partir da raiz do repo
# (`make airflow-up`); o que entra no contexto está em Dockerfile.dockerignore.
#
# Java 21: o mesmo do CI, e uma das versões que o PySpark 4.2.0 suporta (17 e 21). O JRE vem
# da imagem da Temurin com tag exata, sem depender do que o apt do Debian tiver no dia.
FROM eclipse-temurin:21.0.13_9-jre AS java

FROM apache/airflow:3.4.0-python3.12

# As mesmas versões do grupo `spark` do pyproject.toml (um teste confere).
ARG PYSPARK_VERSION=4.2.0
ARG DELTA_SPARK_VERSION=4.4.1
# AWS CLI só para o alvo aws: o perfil do `aws login` pede a credencial a ela.
ARG AWS_CLI_VERSION=2.38.0

USER root
COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/java/openjdk/bin:$PATH
RUN apt-get update \
    && apt-get install -y --no-install-recommends unzip \
    && curl -fsSLo /tmp/awscli.zip \
        "https://awscli.amazonaws.com/awscli-exe-linux-x86_64-${AWS_CLI_VERSION}.zip" \
    && unzip -q /tmp/awscli.zip -d /tmp \
    && /tmp/aws/install \
    && rm -rf /tmp/aws /tmp/awscli.zip \
    && apt-get purge -y unzip && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*
# Pastas que recebem volume: o projeto (data/raw e data_sim montados) e o cache do Ivy, onde o
# Spark guarda os jars do Delta. Grupo 0 com escrita, porque o container roda com o UID do
# usuário da máquina (AIRFLOW_UID) e grupo 0, e o volume novo herda as permissões daqui.
RUN mkdir -p /opt/brasileirao/data/raw /opt/brasileirao/data_sim /home/airflow/.ivy2.5.2 \
    && chown -R airflow:0 /opt/brasileirao /home/airflow/.ivy2.5.2 \
    && chmod -R g+rwX /opt/brasileirao /home/airflow/.ivy2.5.2

USER airflow
COPY --chown=airflow:0 pyproject.toml README.md /tmp/brasileirao/
COPY --chown=airflow:0 src /tmp/brasileirao/src
# O Airflow fixado no mesmo comando, como manda a doc da imagem, para o pip não trocá-lo.
RUN pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        "pyspark==${PYSPARK_VERSION}" \
        "delta-spark==${DELTA_SPARK_VERSION}" \
        /tmp/brasileirao \
    && rm -rf /tmp/brasileirao
