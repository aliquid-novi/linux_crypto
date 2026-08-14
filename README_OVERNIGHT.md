# Fillquality overnight quick start

Goal tonight: collect fill/event/latency data first. Prometheus/node_exporter can wait.

## Fast path

```bash
cd ~/linux_crypto
# copy these files into the repo first
chmod +x scripts/*.sh
./scripts/setup_venv.sh
RUN_MINUTES=480 TAKER_Z=0.9 PROBE_SECS=45 ./scripts/start_fillquality.sh
./scripts/status_fillquality.sh
```

Leave the terminal/computer running. Data will accumulate in:

```text
data/fills.jsonl
data/markouts.jsonl
data/events.jsonl
data/latency.jsonl
data/health.jsonl
runtime/heartbeat.json
logs/fillquality.log
```

If something looks wrong:

```bash
./scripts/incident_snapshot.sh
```

When done:

```bash
./scripts/stop_fillquality.sh
./scripts/archive_run.sh overnight
```

## Optional Prometheus later

Install node_exporter and Prometheus, then use `config/prometheus.yml` to scrape:

- app metrics: localhost:9108
- node_exporter: localhost:9100

For the data-analysis project, JSONL/derived files are more important than Grafana.
