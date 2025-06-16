
#!/bin/bash

echo "Starting PostgreSQL with Docker Compose..."
docker compose up -d postgres

echo "Waiting for PostgreSQL to be ready..."
sleep 10

echo "Installing Python dependencies..."
pip install -r requirements.txt

echo "Starting FastAPI application..."
python main.py
