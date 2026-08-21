# Weather Stations Monitoring — Lab 4

Distributed weather station monitoring system: 10 mock weather station
producers → Apache Kafka → Kafka Streams rain-detection processor + a
central base station consumer that batch-persists readings to PostgreSQL.

## Project layout

```
weather-station/     Java producer (mock station). Same image used for all 10 stations.
central-station/     Java consumer: Kafka Streams rain detector + DB batch writer.
sql/                 schema.sql and analysis_queries.sql
k8s/                 Kubernetes manifests (namespace, zookeeper, kafka, postgres, both services)
docker-compose.yaml  Local end-to-end test environment (no Kubernetes needed)
```

## Message schema

Each station emits, once per second, to the `weather-readings` topic (key = station_id):

```json
{
  "station_id": 1,
  "s_no": 42,
  "battery_status": "low",
  "status_timestamp": 1681521224,
  "weather": { "humidity": 35, "temperature": 100, "wind_speed": 13 }
}
```

`battery_status` is drawn low/medium/high at 30/40/30%. Each sampled reading
has a 10% chance of being dropped (never sent to Kafka), but `s_no` still
increments for the dropped reading — this is what lets the SQL "dropped
messages" analysis compare expected vs. received counts per station.

The Kafka Streams topology (`RainDetectionTopology`) filters `humidity > 70`
and republishes an alert JSON to the `rain-alerts` topic.

## Run locally with Docker Compose

Requires Docker only.

```bash
docker compose up --build
```

This starts Zookeeper, Kafka, PostgreSQL, the central station, and 3 sample
weather stations (station ids 1-3; add more services in
[docker-compose.yaml](docker-compose.yaml) or run additional containers with
different `STATION_ID` values to reach 10).

Check messages are flowing:

```bash
docker exec -it <kafka-container> kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic weather-readings --from-beginning
```

Check persistence:

```bash
docker exec -it <postgres-container> psql -U weather -d weather \
  -c "SELECT COUNT(*) FROM weather_readings;"
```

## Build the Docker images

```bash
docker build -t weather-station:latest ./weather-station
docker build -t central-station:latest ./central-station
```

For Minikube, load the images into the cluster's Docker daemon first
(`eval $(minikube docker-env)` before the builds above, or `minikube image load`).

## Deploy to Kubernetes

```bash
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-zookeeper.yaml
kubectl apply -f k8s/02-kafka.yaml
kubectl apply -f k8s/03-postgres.yaml
kubectl apply -f k8s/04-central-station.yaml
kubectl apply -f k8s/05-weather-station.yaml

kubectl -n weather-lab get pods -w
```

This creates:
- 1 Zookeeper + 1 Kafka broker (Bitnami images)
- 1 PostgreSQL instance backed by a PersistentVolumeClaim
- 1 Central Station Deployment (Kafka Streams processor + DB writer)
- 1 Weather Station **StatefulSet with 10 replicas** (`weather-station-0` …
  `weather-station-9`). The container entrypoint derives `STATION_ID` (1-10)
  from the pod's ordinal suffix, so no manual per-pod configuration is needed.

Update credentials in [k8s/03-postgres.yaml](k8s/03-postgres.yaml) (`postgres-credentials`
Secret) before any real deployment — the checked-in value is a placeholder.

### Verifying the deployment

```bash
kubectl -n weather-lab logs -l app=weather-station --tail=5 --prefix
kubectl -n weather-lab logs deploy/central-station --tail=20
kubectl -n weather-lab exec -it deploy/postgres -- psql -U weather -d weather \
  -c "SELECT station_id, COUNT(*) FROM weather_readings GROUP BY station_id ORDER BY 1;"
```

## SQL

- [sql/schema.sql](sql/schema.sql) — `weather_readings` table (also auto-created
  by the central station on startup).
- [sql/analysis_queries.sql](sql/analysis_queries.sql) — battery status
  distribution per station and dropped-message analysis per station, plus a
  couple of bonus queries (latest status per station, rain-event counts).

## Bonus: Cloud deployment (no Kubernetes)

Not automated by this repo — it requires provisioning real cloud VMs. Outline:

1. **Central Base Station VM**: install Docker, run Zookeeper + Kafka
   (advertise the VM's public/private IP via `KAFKA_CFG_ADVERTISED_LISTENERS`
   so remote producers can connect), run `central-station:latest` pointed at
   `localhost:9092` and at the managed database (below). Open port 9092 to the
   Weather Stations VM's IP only.
2. **Weather Stations VM**: install Docker, run 10 containers of
   `weather-station:latest` with `STATION_ID=1..10` and
   `KAFKA_BOOTSTRAP_SERVERS=<central-vm-ip>:9092`.
3. **Managed database (Aiven)**: create a PostgreSQL service, then point the
   central station at it via `DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` /
   `DB_PASSWORD` environment variables (never hardcoded) and set
   `DB_SSLMODE=require`.
4. Capture screenshots/logs of: stations producing, central station
   consuming + inserting, and a `psql` session against the Aiven database
   showing row counts — these go in the report's Cloud Deployment section.

## Report checklist

The final PDF report should include: architecture diagram, message schema,
design decisions (batch size/flush interval, at-least-once + `ON CONFLICT DO
NOTHING` idempotency, Kafka Streams DSL topology), the two required SQL
analyses with actual result screenshots, Kubernetes deployment
screenshots/logs, and (if attempting bonus) the Cloud Deployment section
described above.
