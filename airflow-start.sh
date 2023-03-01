#!/bin/bash

docker-compose down
kill -9 $(ps aux | grep "[.]listener" | awk '{print $2}')

docker-compose up --build --force-recreate -d
docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' airflow_dask-worker_1 > configs/dask-container.ip

echo done;
