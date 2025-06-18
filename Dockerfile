FROM apache/airflow:2.10.0-python3.9

ENV AIRFLOW_HOME=/opt/airflow

USER root

RUN apt-get update -qq && apt-get install vim -qqq
RUN apt-get update && apt-get install -y \
    r-base \
    build-essential \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    libpq-dev 
RUN Rscript -e "install.packages(c('baseballr', 'dplyr', 'tidyr', 'lubridate', 'progressr', 'stringr', 'readr', 'DBI', 'RPostgres'), repos='https://cloud.r-project.org')"

COPY requirements.txt .

USER airflow

RUN pip install dbt dbt-postgres tweepy
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir pandas sqlalchemy nfl_data_py psycopg2-binary
USER root

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID