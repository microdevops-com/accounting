FROM python:3.9

WORKDIR /opt/sysadmws/accounting

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update
RUN apt-get install -y jq postgresql-client

COPY *.py ./
COPY *.sql ./
COPY accounting.yaml ./
COPY clients ./clients
COPY tariffs ./tariffs
COPY .gitlab-server-job ./.gitlab-server-job
COPY .ssh ./.ssh
