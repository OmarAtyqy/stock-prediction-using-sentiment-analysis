#!/bin/bash

# Stop the current running containers
docker-compose down

# Remove dangling images
docker image prune -f

# Build the new image with the updated requirements
docker build -f Docker/spark/Dockerfile -t stock-prediction-spark .
docker build -f Docker/kafka/Dockerfile -t stock-prediction-kafka .

# Start the services with the new image
docker-compose up --build -d
