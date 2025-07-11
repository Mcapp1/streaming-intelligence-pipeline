#!/bin/bash

# Deployment script for streaming intelligence pipeline
set -e

echo "🚀 Starting deployment of streaming intelligence pipeline..."

# Build images first
echo "📦 Building Docker images..."
docker build -f Dockerfile.producer -t streaming-producer:latest .
docker build -f Dockerfile.consumer -t streaming-consumer:latest .

# Check if images exist
if ! docker images | grep -q "streaming-producer.*latest"; then
    echo "❌ Failed to build producer image"
    exit 1
fi

if ! docker images | grep -q "streaming-consumer.*latest"; then
    echo "❌ Failed to build consumer image"
    exit 1
fi

echo "✅ Images built successfully"

# Apply Kubernetes manifests
echo "🎯 Applying Kubernetes manifests..."

# Apply secrets and config first
kubectl apply -f secrets.yaml

# Apply deployments
kubectl apply -f producer-deployment.yaml
kubectl apply -f consumer-deployment.yaml

echo "⏳ Waiting for deployments to be ready..."

# Wait for deployments
kubectl wait --for=condition=available --timeout=300s deployment/streaming-producer
kubectl wait --for=condition=available --timeout=300s deployment/streaming-consumer

echo "✅ Deployments are ready!"

# Show status
echo "📊 Current status:"
kubectl get pods -l app=streaming-producer
kubectl get pods -l app=streaming-consumer

echo "📝 To check logs:"
echo "kubectl logs -f deployment/streaming-producer"
echo "kubectl logs -f deployment/streaming-consumer"

echo "🎉 Deployment completed successfully!"