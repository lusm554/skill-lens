FROM ghcr.io/mlflow/mlflow:v3.7.0
USER root
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
# Остаемся root, чтобы иметь права на запись в docker volume /data
USER root
