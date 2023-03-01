FROM apache/airflow:2.5.1-python3.10

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends build-essential libsasl2-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir dask[distributed]
RUN pip install --no-cache-dir "bokeh>=2.4.2,<3"
RUN pip install --no-cache-dir "apache-airflow-providers-apache-spark"
RUN pip install --no-cache-dir "apache-airflow-providers-amazon"
RUN pip install --no-cache-dir "apache-airflow-providers-apache-hive"
RUN pip install --no-cache-dir "lightgbm"
