#!/bin/bash

# Ensure Docker is installed
if ! [ -x "$(command -v docker)" ]; then
  echo "Error: Docker is not installed. Install it first." >&2
  exit 1
fi

# Start Docker Compose
docker-compose up --build -d

# Initialize Airflow database
docker-compose run airflow-db-init

# Start Airflow services
docker-compose up -d
