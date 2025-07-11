#!/bin/bash

# Troubleshooting script for Kubernetes issues
echo "🔍 Kubernetes Troubleshooting Report"
echo "=================================="

# Check cluster info
echo "📋 Cluster Info:"
kubectl cluster-info
echo ""

# Check nodes
echo "🖥️ Node Status:"
kubectl get nodes -o wide
echo ""

# Check all pods in default namespace
echo "🚀 Pod Status:"
kubectl get pods -o wide
echo ""

# Check deployments
echo "📦 Deployment Status:"
kubectl get deployments
echo ""

# Check events
echo "📰 Recent Events:"
kubectl get events --sort-by='.lastTimestamp' | tail -10
echo ""

# Check specific pods if they exist
if kubectl get pods | grep -q "streaming-producer"; then
    echo "🔍 Producer Pod Details:"
    kubectl describe pod -l app=streaming-producer
    echo ""
    
    echo "📜 Producer Logs:"
    kubectl logs -l app=streaming-producer --tail=20
    echo ""
fi

if kubectl get pods | grep -q "streaming-consumer"; then
    echo "🔍 Consumer Pod Details:"
    kubectl describe pod -l app=streaming-consumer
    echo ""
    
    echo "📜 Consumer Logs:"
    kubectl logs -l app=streaming-consumer --tail=20
    echo ""
fi

# Check resource usage
echo "💾 Resource Usage:"
kubectl top nodes 2>/dev/null || echo "Metrics server not available"
kubectl top pods 2>/dev/null || echo "Metrics server not available"
echo ""

# Check services
echo "🌐 Services:"
kubectl get services
echo ""

# Check secrets and configmaps
echo "🔐 Secrets and ConfigMaps:"
kubectl get secrets
kubectl get configmaps
echo ""

echo "🎯 Common Issues to Check:"
echo "1. Are Docker images built and available locally?"
echo "2. Is imagePullPolicy set correctly?"
echo "3. Are resource limits appropriate?"
echo "4. Are environment variables accessible?"
echo "5. Is Kafka accessible from pods?"
echo ""

echo "💡 Quick fixes to try:"
echo "kubectl delete deployment streaming-producer streaming-consumer"
echo "kubectl apply -f secrets.yaml"
echo "kubectl apply -f producer-deployment.yaml"
echo "kubectl apply -f consumer-deployment.yaml"