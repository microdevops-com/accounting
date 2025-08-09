FROM python:3.12-bookworm

WORKDIR /opt/sysadmws/accounting

SHELL ["/bin/bash", "-c"]

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y \
    && apt-get -qy install \
    jq \
    rsync \
    postgresql-client \
    python3-yaml \
    python3-psycopg2

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./
COPY *.sql ./
COPY accounting.yaml ./
COPY clients ./clients
COPY tariffs ./tariffs
COPY .gitlab-server-job ./.gitlab-server-job
COPY .ssh ./.ssh
COPY entrypoint.sh /opt/sysadmws/accounting/entrypoint.sh

ENTRYPOINT ["/opt/sysadmws/accounting/entrypoint.sh"]
