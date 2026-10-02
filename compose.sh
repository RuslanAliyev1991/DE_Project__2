#!/bin/bash

#service docker start

read -p "Enter up/down (or down -v):" status

if [ $status = "up" ]; then
	docker network create networkRealTimeStreaming_project2 2>/dev/null || true
	echo "networkRealTimeStreaming_project2 is created"
	echo "containers up..."
	#docker compose -f ./airflow/docker-compose.yml up -d
	docker compose -f ./kafka/docker-compose.yml up -d
	docker compose -f ./metabase/docker-compose.yml up -d
	docker compose -f ./source/docker-compose.yml up -d
	docker compose -f ./spark/docker-compose.yml up -d
	#docker compose -f ./airflow/docker-compose.yml up airflow-init
	
	echo "up success"

elif [ $status = "down" ]; then
	echo "containers down..."
	#docker compose -f ./airflow/docker-compose.yml down
	docker compose -f ./kafka/docker-compose.yml down
	docker compose -f ./metabase/docker-compose.yml down
	docker compose -f ./source/docker-compose.yml down
	docker compose -f ./spark/docker-compose.yml down
	echo "down success"
	docker network rm networkRealTimeStreaming_project2 2>/dev/null || true
	echo "networkRealTimeStreaming_project2 is removed"

else
    echo "containers down -v..."
	#docker compose -f ./airflow/docker-compose.yml down -v
	docker compose -f ./kafka/docker-compose.yml down -v
	docker compose -f ./metabase/docker-compose.yml down -v
	docker compose -f ./source/docker-compose.yml down -v
	docker compose -f ./spark/docker-compose.yml down -v
	#docker volume prune -y
	echo "down -v success"
	docker network rm networkRealTimeStreaming_project2 2>/dev/null || true
	echo "networkRealTimeStreaming_project2 is removed"
fi
