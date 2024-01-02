#!/bin/bash

# Check if stock name and interval are provided
if [ "$#" -ne 2 ]; then
    echo "Usage: ./run.sh <stock_name> <interval>"
    exit 1
fi

STOCK_NAME=$1
INTERVAL=$2

# Start the dashboard service in the background
docker-compose exec -d dash-app bash -c "cd /mnt && python3 -m src.dashboard.main_dashboard" &
sleep 5

# Start the prediction service in the background
docker-compose exec -d spark-master bash -c "cd /mnt && python3 -m src.spark.main_pipeline" &
sleep 5

# Start the data fetching service
docker-compose exec -it kafka bash -c "cd /mnt && python3 -m src.kafka.main $STOCK_NAME $INTERVAL"
