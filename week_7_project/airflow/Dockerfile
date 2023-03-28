# First-time build can take up to 10 mins.

FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq
# git gcc g++ -qqq

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir pandas


# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    # curl (short for "Client URL") is a command line tool that enables data transfer over various network protocols.
    # -f: If the server returns an error, curl fails silently and returns error 22.
    # -L: Allow curl to follow any redirections. 
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    # -p: Creates nested directories, but only if they don't exist already.
    && mkdir -p "${GCLOUD_HOME}" \
    # The Linux 'tar' stands for tape archive, is used to create Archive and extract the Archive files.
    # -x: Extract the archive.
    # -z: zip, tells tar command that creates tar file using gzip .
    # -f: creates archive with given filename.
    # -c: Creates an archive file.
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

# Installing Java

ARG OPENJDK_VERSION=11.0.2
ENV JAVA_HOME="${HOME}/spark/jdk-${OPENJDK_VERSION}"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN DOWNLOAD_URL="https://download.java.net/java/GA/jdk11/9/GPL/openjdk-${OPENJDK_VERSION}_linux-x64_bin.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/openjdk.tar.gz" \
    && mkdir -p "${JAVA_HOME}" \
    && tar xzf "${TMP_DIR}/openjdk.tar.gz" -C "${JAVA_HOME}" --strip-components=1 \
    && rm -rf "${TMP_DIR}" \
    && java --version

# Installing Spark

ARG SPARK_VERSION=3.1.3

ENV SPARK_HOME="${HOME}/spark/spark-${SPARK_VERSION}-bin-hadoop3.2"

RUN DOWNLOAD_URL="https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz" \
    && mkdir -p "${SPARK_HOME}" \
    && tar xzfv "${TMP_DIR}/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz" -C "${SPARK_HOME}" --strip-components=1 \
    && rm -rf "${TMP_DIR}"

ENV PATH="${SPARK_HOME}/bin:${PATH}"

ENV PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH"

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID