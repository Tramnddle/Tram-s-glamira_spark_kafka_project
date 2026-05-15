#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR=/spark

docker_remove_if_exists() {
  local container_name="$1"

  if docker container inspect "$container_name" >/dev/null 2>&1; then
    docker container rm -f "$container_name" >/dev/null
  fi
}

set -a
source "${SCRIPT_DIR}/.env"
set +a

: "${POSTGRES_HOST:?POSTGRES_HOST must be set in .env}"
: "${POSTGRES_PORT:?POSTGRES_PORT must be set in .env}"
: "${POSTGRES_DB:?POSTGRES_DB must be set in .env}"
: "${POSTGRES_USER:?POSTGRES_USER must be set in .env}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD must be set in .env}"

docker_remove_if_exists postgres-loader

docker run --rm -ti --name postgres-loader \
--user root \
--network=streaming-network \
--env-file "${SCRIPT_DIR}/.env" \
-v "${SCRIPT_DIR}:/spark" \
-v /home/tramnguyen/Documents/GitHub/de-coaching-lab/spark:/external_spark \
-v spark_lib:/home/spark/.ivy2 \
-v spark_data:/data \
-e HADOOP_CONF_DIR=${PROJECT_DIR}/hadoop_conf/ \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
-e POSTGRES_JDBC_VERSION="${POSTGRES_JDBC_VERSION:-42.7.3}" \
-e POSTGRES_JDBC_JAR_URL="${POSTGRES_JDBC_JAR_URL:-}" \
-e POSTGRES_TRIGGER_INTERVAL="${POSTGRES_TRIGGER_INTERVAL:-30 seconds}" \
-e POSTGRES_MAX_FILES_PER_TRIGGER="${POSTGRES_MAX_FILES_PER_TRIGGER:-10}" \
unigap/spark:3.5 bash -c "(cd ${PROJECT_DIR} && zip -r /tmp/project_deps.zip util browser) &&
conda env create --file ${PROJECT_DIR}/environment.yml &&
eval \"\$(conda shell.bash hook)\" &&
conda activate pyspark_conda_env &&
conda pack -f -o pyspark_conda_env.tar.gz &&
: \"\${POSTGRES_HOST:?POSTGRES_HOST must be set in .env}\" &&
: \"\${POSTGRES_PORT:?POSTGRES_PORT must be set in .env}\" &&
: \"\${POSTGRES_DB:?POSTGRES_DB must be set in .env}\" &&
: \"\${POSTGRES_USER:?POSTGRES_USER must be set in .env}\" &&
: \"\${POSTGRES_PASSWORD:?POSTGRES_PASSWORD must be set in .env}\" &&
POSTGRES_JDBC_JAR_URL=\"\${POSTGRES_JDBC_JAR_URL:-https://repo1.maven.org/maven2/org/postgresql/postgresql/\${POSTGRES_JDBC_VERSION}/postgresql-\${POSTGRES_JDBC_VERSION}.jar}\" &&
curl -fsSL \"\${POSTGRES_JDBC_JAR_URL}\" -o /tmp/postgresql-\${POSTGRES_JDBC_VERSION}.jar &&
spark-submit \
--jars /tmp/postgresql-\${POSTGRES_JDBC_VERSION}.jar \
--driver-class-path /tmp/postgresql-\${POSTGRES_JDBC_VERSION}.jar \
--conf spark.yarn.dist.archives=pyspark_conda_env.tar.gz#environment \
--py-files /tmp/project_deps.zip \
--deploy-mode client \
--master yarn \
${PROJECT_DIR}/load_to_db.py"
