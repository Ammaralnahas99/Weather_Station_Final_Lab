# Bonus: Cloud Deployment (no Kubernetes)

Deploys the same system across **two AWS EC2 free-tier VMs** plus a
**managed Aiven PostgreSQL** database, as required by the assignment bonus.

```
Weather Stations VM (10 containers)  --Kafka (9092)-->  Central Station VM
                                                          (Zookeeper + Kafka + central-station)
                                                                    |
                                                                    v
                                                          Aiven PostgreSQL (internet, SSL)
```

## 1. Create the two EC2 instances (AWS Free Tier)

1. Sign up / log in at https://aws.amazon.com (Free Tier eligible account).
2. EC2 console → **Launch instance**, twice, with:
   - AMI: **Ubuntu Server 22.04 LTS** (or 24.04)
   - Instance type: **t3.micro** (or t2.micro) — free-tier eligible
   - Key pair: create one (e.g. `weather-lab-key.pem`) and reuse it for both instances
   - Network: put **both instances in the same default VPC and subnet** (same
     Availability Zone) so they can talk over private IPs
   - Name them clearly: `central-station-vm` and `weather-stations-vm`
3. Security groups:
   - `central-station-vm`: allow inbound **22 (SSH)** from your IP only, and
     **9092 (Kafka)** from the `weather-stations-vm` security group (reference
     the SG, not `0.0.0.0/0` — no need to expose Kafka publicly since both VMs
     share a VPC)
   - `weather-stations-vm`: allow inbound **22 (SSH)** from your IP only (it
     doesn't need to accept any inbound app traffic, it only calls out)
4. Note down both instances' **private IPv4 addresses** (EC2 console → instance
   → Details) — you'll need the central VM's private IP shortly. Note the
   **public IPv4 addresses** too, for SSH access from your machine.

## 2. Create the Aiven PostgreSQL service

1. Sign up at https://aiven.io (free trial / free plan).
2. **Create service** → PostgreSQL → pick the free plan → any region → create.
3. Wait for the service to go "Running", then open its **Overview** tab and
   copy: Host, Port, Database name (`defaultdb`), User (`avnadmin`), Password.
4. This database is reachable over the public internet with SSL already
   enforced by Aiven — that's what `DB_SSLMODE=require` in
   [central.env.example](central.env.example) is for.

## 3. Bootstrap both VMs

SSH into each instance and install Docker:

```bash
ssh -i weather-lab-key.pem ubuntu@<PUBLIC_IP>
git clone <your-repo-url> weather-lab
cd weather-lab/cloud
bash bootstrap.sh
newgrp docker   # or log out and back in
```

## 4. Configure and start the Central Station VM

On `central-station-vm`:

```bash
cd ~/weather-lab/cloud
cp central.env.example central.env
nano central.env   # KAFKA_ADVERTISED_HOST=<central VM's PRIVATE IP>, plus the Aiven DB_* values

docker compose -f docker-compose.central.yml --env-file central.env up -d --build
docker compose -f docker-compose.central.yml ps
docker compose -f docker-compose.central.yml logs -f central-station
```

## 5. Configure and start the Weather Stations VM

On `weather-stations-vm`:

```bash
cd ~/weather-lab/cloud
cp stations.env.example stations.env
nano stations.env   # KAFKA_HOST=<central VM's PRIVATE IP>  (same value as step 4)

docker compose -f docker-compose.stations.yml --env-file stations.env up -d --build
docker compose -f docker-compose.stations.yml ps
docker compose -f docker-compose.stations.yml logs -f weather-station-1
```

## 6. Verify end-to-end

From the central VM:

```bash
# Kafka is receiving readings from the remote stations
docker exec -it cloud-kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9093 --topic weather-readings --max-messages 5

# central-station is batch-inserting into Aiven
docker compose -f docker-compose.central.yml logs central-station | grep Persisted
```

From your own machine (or the Aiven web console's SQL editor), connect to
Aiven directly and confirm rows are landing:

```bash
psql "host=<DB_HOST> port=<DB_PORT> dbname=defaultdb user=avnadmin sslmode=require" \
  -c "SELECT station_id, COUNT(*) FROM weather_readings GROUP BY station_id ORDER BY 1;"
```

**For the report**, capture screenshots/logs of:
- Both EC2 instances running (EC2 console)
- `docker compose ps` output on each VM (all containers `Up`)
- The Kafka console-consumer output showing live readings
- The `psql` (or Aiven console) query above showing per-station row counts
- The Aiven service Overview page (proves it's a managed, externally-hosted DB)

## 7. Tear down (avoid any charges after you're done)

```bash
# On each VM
docker compose -f docker-compose.<central|stations>.yml down
```
Then terminate both EC2 instances from the console and delete the Aiven
service from its console.
