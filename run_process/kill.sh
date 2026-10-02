#!/bin/bash
echo "process are stopped..."
docker exec -i spark-master pkill -f stream.py || true
pkill -f api_kafka.py