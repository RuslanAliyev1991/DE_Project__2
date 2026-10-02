#!/bin/bash

echo "process are started..."
source ../spark311_venv/bin/activate
python3 ./api_kafka.py >> ./logs/api_debug.log 2>&1 &

sleep 10

docker exec -i spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/jars/jars/* /opt/spark/app/stream.py >> ./logs/spark_debug.log 2>&1 &