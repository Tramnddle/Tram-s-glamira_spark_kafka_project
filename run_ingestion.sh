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

: "${SOURCE_KAFKA_BOOTSTRAP_SERVERS:?SOURCE_KAFKA_BOOTSTRAP_SERVERS must be set in .env}"
: "${SOURCE_KAFKA_SECURITY_PROTOCOL:?SOURCE_KAFKA_SECURITY_PROTOCOL must be set in .env}"
: "${SOURCE_KAFKA_SASL_MECHANISM:?SOURCE_KAFKA_SASL_MECHANISM must be set in .env}"
: "${SOURCE_KAFKA_USERNAME:?SOURCE_KAFKA_USERNAME must be set in .env}"
: "${SOURCE_KAFKA_PASSWORD:?SOURCE_KAFKA_PASSWORD must be set in .env}"

docker_remove_if_exists kafka-streaming

docker run --rm -ti --name kafka-streaming \
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
unigap/spark:3.5 bash -c "(cd ${PROJECT_DIR} && zip -r /tmp/project_deps.zip util browser) &&
conda env create --file ${PROJECT_DIR}/environment.yml &&
eval \"\$(conda shell.bash hook)\" &&
conda activate pyspark_conda_env &&
conda pack -f -o pyspark_conda_env.tar.gz &&
export KAFKA_BOOTSTRAP_SERVERS=\"\${SOURCE_KAFKA_BOOTSTRAP_SERVERS:?SOURCE_KAFKA_BOOTSTRAP_SERVERS must be set in .env}\" &&
export KAFKA_SECURITY_PROTOCOL=\"\${SOURCE_KAFKA_SECURITY_PROTOCOL:?SOURCE_KAFKA_SECURITY_PROTOCOL must be set in .env}\" &&
export KAFKA_SASL_MECHANISM=\"\${SOURCE_KAFKA_SASL_MECHANISM:?SOURCE_KAFKA_SASL_MECHANISM must be set in .env}\" &&
export KAFKA_SASL_JAAS_CONFIG=\"org.apache.kafka.common.security.plain.PlainLoginModule required username=\\\"\${SOURCE_KAFKA_USERNAME:?SOURCE_KAFKA_USERNAME must be set in .env}\\\" password=\\\"\${SOURCE_KAFKA_PASSWORD:?SOURCE_KAFKA_PASSWORD must be set in .env}\\\";\" &&
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
--conf spark.yarn.dist.archives=pyspark_conda_env.tar.gz#environment \
--py-files /tmp/project_deps.zip \
--deploy-mode client \
--master yarn \
${PROJECT_DIR}/ingestion_transformation.py"
