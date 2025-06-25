#!/bin/bash

set -e

echo "🔧 Starting deployment of Generative AI system..."

PROJECT_ROOT="$(pwd)"
PRODUCER_DIR="$PROJECT_ROOT/producer-server"
CONSUMER_DIR="$PROJECT_ROOT/consumer-server"

install_node_deps() {
  local DIR="$1"
  echo "📦 Checking dependencies in $DIR..."
  if [ ! -f "$DIR/package.json" ]; then
    echo "📁 package.json not found in $DIR. Initializing..."
    cd "$DIR"
    npm init -y
    npm install express kafka-node axios redis
    echo "✅ Initialized and installed dependencies in $DIR."
    cd "$PROJECT_ROOT"
  else
    echo "✅ package.json already exists in $DIR. Skipping init."
  fi
}

# Step 1: Prepare Node.js dependencies for producer and consumer
install_node_deps "$PRODUCER_DIR"
install_node_deps "$CONSUMER_DIR"

# Step 2: Clean up any old containers
echo "🧹 Cleaning up previous containers..."
docker-compose down --remove-orphans

# Step 3: Build all images (without cache)
echo "🏗️ Building Docker images (no-cache)..."
docker-compose build --no-cache

# Step 4: Start all services
echo "🚀 Starting all services..."
docker-compose up -d

# Step 5: Show status
echo "📋 Running containers:"
docker ps

echo "✅ Deployment completed successfully."
