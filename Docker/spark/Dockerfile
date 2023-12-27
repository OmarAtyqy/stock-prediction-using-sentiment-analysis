# Start from the base Spark image
FROM bitnami/spark:latest

# Update and install python and pip
USER root
RUN apt-get update && \
    apt-get install -y python3 python3-pip

# Upgrade pip
RUN pip3 install --upgrade pip

# Copy your requirements.txt file into the image
COPY requirements.txt /tmp/

# Install Python dependencies from requirements.txt
RUN pip3 install -r /tmp/requirements.txt