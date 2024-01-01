#!/bin/bash

# Stop the current running containers
docker-compose down

# Remove Kafka volume (adjust volume name as necessary)
docker volume rm stock-prediction-using-sentiment-analysis_kafka-volume

# Remove dangling images
docker image prune -f

# Build the new images
docker build -f Docker/spark/Dockerfile -t stock-prediction-spark .
docker build -f Docker/kafka/Dockerfile -t stock-prediction-kafka .
docker build -f Docker/dashboard/Dockerfile -t stock-prediction-dash .

# Start all services with new images
docker-compose up --build -d
