#!/bin/bash
set -e

NAMESPACE="project"

echo "==> Limpar cluster antigo "

minikube stop || true
minikube delete || true
docker system prune -af
docker volume prune -f

echo "==> Ativar minikube"
minikube start --driver=docker

echo "==> Importar imagens locais para o docker do minikube"
eval $(minikube docker-env)
docker build -t sql-postgres:latest ./db
docker build -t kafka-producer:latest ./kafka_app
docker build -t spark-job:latest ./spark

echo "==> Criar namespace"
kubectl create namespace "${NAMESPACE}" || true
kubectl config set-context --current --namespace="${NAMESPACE}"

echo "==> Aplicar ficheiros yaml"
kubectl apply -n project -f kubernetes/

echo "A aguardar ficar pronto..."
sleep 300

echo "==> Verificar"
kubectl get all -n project 
kubectl get pvc -n project

echo "==> Expor Grafana"
kubectl port-forward svc/grafana 3000:3000 -n project 
