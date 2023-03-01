FROM python:3.10.5-slim
#FROM openjdk:18.0.2.1-slim-buster

USER 0

# add java to image
COPY --from=openjdk:8-jre-slim /usr/local/openjdk-8 /usr/local/openjdk-8
ENV JAVA_HOME /usr/local/openjdk-8
RUN update-alternatives --install /usr/bin/java java /usr/local/openjdk-8/bin/java 1
ENV JAVA_HOME=""

# setup global env
ENV DASK_HOST=localhost
ENV DASK_PORT=8786
ENV AIRFLOW_HOME=/opt/airflow
ENV SPARK_HOME=/apps/spark-3.2.1-bin-hadoop3.2/

# install addition package
RUN apt-get update -y \
 && apt-get install -y --no-install-recommends build-essential libsasl2-dev musl-dev openssh-client sshpass \
 && apt-get autoremove -yqq --purge \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# install requirements python packages
RUN pip install --no-cache-dir dask[distributed] 
RUN pip install --no-cache-dir apache-airflow
RUN pip install --no-cache-dir apache-airflow-providers-amazon
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark
RUN pip install --no-cache-dir apache-airflow-providers-apache-hive
RUN pip install --no-cache-dir psycopg2-binary
RUN pip install --no-cache-dir lightgbm
RUN pip install --no-cache-dir pyspark==3.2.1
#RUN pip install --no-cache-dir scikit-learn==1.1.2 pandas 
#RUN pip install --no-cache-dir xgboost lightgbm matplotlib seaborn

# setup for ssh to host
#RUN mkdir /root/.ssh
#RUN ssh-keyscan IPofHOST | grep '${IPofHOST} ssh-rsa' > ~/.ssh/known_hosts

# expose port
EXPOSE 8786 4040
EXPOSE 10000-50000

# set current workdir
WORKDIR /opt/airflow

# ENTRYPOINT dask scheduler --host $DASK_HOST --port $DASK_PORT
