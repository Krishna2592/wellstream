# Observability Add-On

This guide explains how to add Prometheus and Grafana dashboards to the WellStream demo for both Docker Compose and Kubernetes environments.

## Docker Compose Setup

1. Build the application images (if you have not already):
   ```powershell
   mvn clean package -DskipTests
   ```
2. Start the base stack plus monitoring services:
   ```powershell
   docker compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d
   ```
3. Open Grafana at `http://localhost:3000` (user `admin`, password `admin`).
4. Add Prometheus as a data source (`http://prometheus:9090`) and import dashboards from `monitoring/grafana-dashboards`.

Available targets:
- `wellstream-spark:4040` exposes Spark metrics at `/metrics/prometheus`.
- `kafka-exporter:9308` exposes Kafka broker metrics.
- `redis-exporter:9121` exposes Redis metrics.

Stop the stack when finished:
```powershell
docker compose -f docker-compose.yml -f docker-compose.monitoring.yml down
```

## Kubernetes Setup

1. Apply the base application manifests as usual:
   ```powershell
   kubectl apply -f k8s
   ```
2. Deploy the monitoring stack (Prometheus, Grafana, exporters):
   ```powershell
   kubectl apply -f k8s/monitoring/monitoring-stack.yml
   ```
3. Expose Grafana locally:
   ```powershell
   kubectl port-forward svc/grafana -n monitoring 3000:80
   ```
   Access Grafana at `http://localhost:3000` (admin/admin).
4. Add Prometheus as a data source at `http://prometheus.monitoring.svc.cluster.local:9090`.
5. Import dashboards from `monitoring/grafana-dashboards`.

### Targets Scraped in Kubernetes
- `spark-processor-metrics.default.svc.cluster.local:4040`
- `kafka-exporter.monitoring.svc.cluster.local:9308`
- `redis-exporter.monitoring.svc.cluster.local:9121`

### Clean Up
```powershell
kubectl delete -f k8s/monitoring/monitoring-stack.yml
```

## Spark Metrics Configuration

The Spark processor image now ships with `spark-processor/conf/metrics.properties` and enables Prometheus metrics through `spark.ui.prometheus.enabled=true`. Metrics are exposed on port `4040` for both Docker and Kubernetes runtimes.

## Kafka and Redis Exporters

- Kafka metrics are published by `danielqsj/kafka-exporter` and require no changes to the existing broker configuration.
- Redis metrics are published by `oliver006/redis_exporter` and connect to the existing Redis service.

Use these dashboards to demonstrate rate, lag, and error trends during interviews or demos.
