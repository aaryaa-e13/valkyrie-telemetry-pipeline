# VALKYRIE Spark image: Python 3.11 + OpenJDK 17 + PySpark 4.1.4 (local mode).
# Build context: repository root (see docker-compose.yml).
#
# Why not the official apache/spark image? We need Python exactly 3.11 (project
# standard) and could not confirm which Python the official Ubuntu image ships.
FROM python:3.11.18-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Java 17 (Spark 4.1 supports 17/21). procps provides `ps`, used by Spark scripts.
# Note: the Debian package version is whatever bookworm ships at build time; the
# Java *major* version (17) is what Spark's compatibility depends on.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

WORKDIR /app

COPY requirements/spark.txt requirements/spark.txt
RUN pip install -r requirements/spark.txt

# Kafka connector: must match the Spark version exactly.
ARG SPARK_KAFKA_PACKAGE=org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.4
COPY docker/spark/fetch_kafka_jars.sh /tmp/fetch_kafka_jars.sh
RUN bash /tmp/fetch_kafka_jars.sh "$SPARK_KAFKA_PACKAGE" && rm /tmp/fetch_kafka_jars.sh

COPY common/ common/
COPY streaming/ streaming/

# Unprivileged user with uid 1000 so files in the mounted ./data are owned by you.
RUN useradd --uid 1000 --create-home appuser
USER appuser
ENV PYTHONPATH=/app

CMD ["python", "-m", "streaming.raw_to_parquet"]
