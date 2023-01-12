# FROM gcr.io/sys-2b0109it/demo/bitnami/python:3.9
FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=UTF-8
ENV PYTHONPATH /parent
ARG MLMODEL_EXTRA_REQUIREMENTS_TXT=requirements.txt

WORKDIR /parent
COPY requirements.txt ./base-requirements.txt
COPY $MLMODEL_EXTRA_REQUIREMENTS_TXT ./extra-requirements.txt

RUN apt-get clean && apt-get update \
    && apt-get install g++ -y \
    && apt-get install gcc -y \
    && apt-get install git -y \
    && apt-get install -y default-libmysqlclient-dev \
    && apt-get clean && \
    pip3 install --no-cache-dir  -r base-requirements.txt -r extra-requirements.txt

COPY scdfutils ./scdfutils

ENTRYPOINT ["python", "/parent/scdfutils/prepare_run_model.py"]