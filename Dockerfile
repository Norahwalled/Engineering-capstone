FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install --no-install-recommends -y openjdk-17-jre-headless \
    && java_home_path="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" \
    && mkdir -p /opt/java \
    && ln -s "$java_home_path" /opt/java/openjdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY --chown=airflow:root pyproject.toml README.md /opt/capstone/
COPY --chown=airflow:root src /opt/capstone/src
COPY --chown=airflow:root airflow /opt/capstone/airflow
COPY --chown=airflow:root examples /opt/capstone/examples
WORKDIR /opt/capstone
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch==2.5.1 \
    && pip install --no-cache-dir .

ENV PYTHONUNBUFFERED=1 \
    JAVA_HOME=/opt/java/openjdk \
    PYTHONPATH=/opt/capstone:/opt/capstone/src
