# VALKYRIE Spark image: Python 3.11 + OpenJDK 17 + PySpark 4.1.4 + Delta 4.4.1 (local mode).
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
COPY docker/spark/pip_gate.py /tmp/pip_gate.py
# Gate: `pip freeze` must equal the lock file exactly, and pyspark must still be
# 4.1.4 (the resolver must not move it to satisfy delta-spark). Fails the build.
RUN pip install -r requirements/spark.txt \
    && python /tmp/pip_gate.py requirements/spark.txt --expect pyspark==4.1.4 \
    && mkdir -p /opt/valkyrie && pip freeze > /opt/valkyrie/pip-freeze.txt \
    && rm /tmp/pip_gate.py

# Extra jars, resolved at build time (never at runtime). The add/skip list of each
# run is kept in /opt/valkyrie/jars-<name>.txt. Later runs also skip anything an
# earlier run added. Details and the reasons for every exclusion: DECISIONS D2.1.
COPY docker/spark/fetch_jars.sh /tmp/fetch_jars.sh
# Kafka connector: must match the Spark version exactly.
ARG SPARK_KAFKA_PACKAGE=org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.4
RUN bash /tmp/fetch_jars.sh "$SPARK_KAFKA_PACKAGE" kafka
# Delta Lake: the Spark-4.1 build (`_4.1_` in the name). Do NOT use the unsuffixed
# delta-spark_2.13: in Delta 4.4.x that one is built for Spark 4.2.
# Excluded (each would duplicate classes or versions Spark already ships):
#   commons-logging  (via hadoop-client-runtime; Spark uses jcl-over-slf4j instead)
#   shims            (via RoaringBitmap 0.9.25; Spark's RoaringBitmap 1.3.0 has it)
#   jackson-datatype-jdk8 2.13.5 (replaced below by 2.21.2 to match jackson-databind)
ARG DELTA_PACKAGE=io.delta:delta-spark_4.1_2.13:4.4.1
ARG DELTA_EXCLUDES=commons-logging:commons-logging,org.roaringbitmap:shims,com.fasterxml.jackson.datatype:jackson-datatype-jdk8
RUN bash /tmp/fetch_jars.sh "$DELTA_PACKAGE" delta "$DELTA_EXCLUDES"
# Jackson module used by Delta Kernel, aligned with Spark's jackson-databind 2.21.2.
ARG JACKSON_JDK8_PACKAGE=com.fasterxml.jackson.datatype:jackson-datatype-jdk8:2.21.2
RUN bash /tmp/fetch_jars.sh "$JACKSON_JDK8_PACKAGE" jackson && rm /tmp/fetch_jars.sh

# Integrity gate: every added jar must match the committed SHA-256 lock (Ivy does
# not verify checksums). Fails on a mismatch, an unlisted jar or a missing jar.
# JARS_LOCK_VERIFY=0 is ONLY for `make jars-lock` (separate image tag); such an
# image has no lock-verified marker, and delta_compat_check C2 rejects it.
COPY docker/spark/jars_lock.py docker/spark/jars.sha256 /opt/valkyrie/
ARG JARS_LOCK_VERIFY=1
RUN if [ "$JARS_LOCK_VERIFY" = "1" ]; then \
        python /opt/valkyrie/jars_lock.py verify --lock /opt/valkyrie/jars.sha256 \
            --jars-dir "$(python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')/jars" \
            --marker /opt/valkyrie/lock-verified.txt /opt/valkyrie/jars-*.txt; \
    else \
        echo "WARNING: jar lock verification SKIPPED (JARS_LOCK_VERIFY=$JARS_LOCK_VERIFY)"; \
    fi

COPY common/ common/
COPY streaming/ streaming/

# Unprivileged user with uid 1000 so files in the mounted ./data are owned by you.
RUN useradd --uid 1000 --create-home appuser
USER appuser
ENV PYTHONPATH=/app

CMD ["python", "-m", "streaming.raw_to_parquet"]
