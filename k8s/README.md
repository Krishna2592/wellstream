# Kubernetes Deployment

This directory contains the Kubernetes manifests for deploying the WellStream application to a Kubernetes cluster (e.g., Minikube, Kind, AKS, EKS, GKE).

## Overview

Analogy: Kitchen vs Restaurant Manager
- Docker is like a chef who can cook a dish (container).
- Kubernetes is like a restaurant manager who decides:
- How many chefs to hire (replicas)
- What to do if one chef quits (self-healing)
- How to serve dishes to customers (service discovery)
- When to scale up during rush hour (autoscaling)

These files define the desired state of the application in Kubernetes:
- **Deployments**: Manage stateless applications (like our producer and Spark processor) and ensure a specified number of replicas (Pods) are running.
- **StatefulSets**: Manage stateful applications (like Kafka and Zookeeper).
- **Services**: Provide a stable network endpoint (a single DNS name) to access a set of Pods.

## How to Deploy

1. **Prerequisites**: You need a running Kubernetes cluster and `kubectl` configured to connect to it. You also need to have built the Docker images for the `producer` and `spark-processor` and pushed them to a registry that your cluster can access.

2. **Apply Manifests**: Navigate to this directory and run:
   ```bash
   kubectl apply -f .
   ```
   This command will create all the defined resources in your cluster.

## Observability Add-On

- Deploy Prometheus, Grafana, and exporter pods: `kubectl apply -f monitoring/monitoring-stack.yml`.
- Expose Grafana locally when you need the UI: `kubectl port-forward svc/grafana -n monitoring 3000:80` (credentials `admin/admin`).
- Prometheus scrapes the Spark processor through the `spark-processor-metrics` service and reaches Kafka/Redis statistics through the exporter services in the `monitoring` namespace.
- Tear down the monitoring stack when you are done: `kubectl delete -f monitoring/monitoring-stack.yml`.

