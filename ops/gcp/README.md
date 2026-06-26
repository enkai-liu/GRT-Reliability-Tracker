# GCP VM Deployment

This deploys the collector to a small Compute Engine VM and runs it with `systemd`.

## Defaults

The deployment script uses:

```text
Project: grt-reliability-raw-data
Bucket: grt-reliability-raw-data
VM name: grt-collector-vm
Zone: us-east1-b
Machine type: e2-micro
OS: Debian 12
Service account: grt-collector
```

The VM service account is granted `roles/storage.objectAdmin` on the bucket so the collector can upload snapshots without local user credentials.

## Deploy

```bash
chmod +x ops/gcp/deploy_to_vm.sh ops/gcp/install_on_vm.sh
ops/gcp/deploy_to_vm.sh
```

To override defaults:

```bash
ZONE=us-east1-c MACHINE_TYPE=e2-micro ops/gcp/deploy_to_vm.sh
```

## Check Status

```bash
gcloud compute ssh grt-collector-vm --zone us-east1-b \
  --command "systemctl status grt-collector.service --no-pager"
```

## Watch Logs

```bash
gcloud compute ssh grt-collector-vm --zone us-east1-b \
  --command "journalctl -u grt-collector.service -f"
```

## Run Health Check

```bash
gcloud compute ssh grt-collector-vm --zone us-east1-b \
  --command "/opt/grt-reliability-tracker/collector/.venv/bin/python /opt/grt-reliability-tracker/collector/health_check.py"
```

## One-Week Check

When you come back after a few days, run these first:

```bash
collector/.venv/bin/python collector/health_check.py
```

```bash
gcloud compute ssh grt-collector-vm --zone us-east1-b \
  --command "systemctl status grt-collector.service grt-daily-parse.timer grt-weather-forecast.timer --no-pager"
```

```bash
gcloud compute ssh grt-collector-vm --zone us-east1-b \
  --command "systemctl list-timers grt-daily-parse.timer grt-weather-forecast.timer --no-pager"
```

## Parse Job

The VM installs parse service files, but the daily parse timer is disabled by default on the `e2-micro` collector VM. Parsing full days can be CPU and memory heavy, so the safer setup is to keep the small VM focused on raw collection and run parsing locally or on a larger machine.

```text
grt-daily-parse.timer
grt-daily-parse.service
```

Run parsing locally:

```bash
collector/.venv/bin/python collector/parse_snapshots.py --date YYYY-MM-DD --sync-from-gcs --upload-to-gcs --overwrite
collector/.venv/bin/python collector/parse_static_gtfs.py --date YYYY-MM-DD --sync-from-gcs --upload-to-gcs --overwrite
```

Check whether the VM timer is enabled:

```bash
gcloud compute ssh grt-collector-vm --zone us-east1-b \
  --command "systemctl list-timers grt-daily-parse.timer --no-pager"
```

Run the parse job manually for a date:

```bash
gcloud compute ssh grt-collector-vm --zone us-east1-b \
  --command "sudo -u grtcollector /opt/grt-reliability-tracker/ops/gcp/run_daily_parse.sh YYYY-MM-DD"
```

Watch parse logs:

```bash
gcloud compute ssh grt-collector-vm --zone us-east1-b \
  --command "journalctl -u grt-daily-parse.service -f"
```

To enable automatic daily parsing later, use a larger VM first, then run:

```bash
gcloud compute ssh grt-collector-vm --zone us-east1-b \
  --command "sudo systemctl enable --now grt-daily-parse.timer"
```

## Weather Forecast Job

The VM collects ECCC Kitchener-Waterloo and Cambridge forecast snapshots every 3 hours:

```text
grt-weather-forecast.timer
grt-weather-forecast.service
```

Check the timer:

```bash
gcloud compute ssh grt-collector-vm --zone us-east1-b \
  --command "systemctl list-timers grt-weather-forecast.timer --no-pager"
```

Run a forecast snapshot manually:

```bash
gcloud compute ssh grt-collector-vm --zone us-east1-b \
  --command "sudo -u grtcollector /opt/grt-reliability-tracker/collector/.venv/bin/python /opt/grt-reliability-tracker/collector/collect_weather_forecasts.py"
```

Watch forecast logs:

```bash
gcloud compute ssh grt-collector-vm --zone us-east1-b \
  --command "journalctl -u grt-weather-forecast.service -f"
```

## Live Predictions (real-time on the static site)

The GitHub Pages dashboard ships with a frozen `live-predictions.json` snapshot, so its live panel shows "stale". To make it real-time, run the live scorer on the VM and have it upload fresh predictions to a public bucket the dashboard fetches:

```text
VM: predict_live.py --interval-seconds 300
      └─ uploads live/live-predictions.json to GCS_LIVE_BUCKET (public, no-cache, CORS)
            └─ dashboard fetches that URL (window.GRT_CONFIG.liveUrl in index.html)
```

`install_on_vm.sh` installs `grt-live-scorer.service` but leaves it disabled until the steps below are done.

### 1. Prerequisites on the VM

The scorer needs the trained model and parsed static GTFS present on the VM:

```bash
# trained model (lgbm_model.txt + any quantile models)
gcloud compute scp --recurse data/analysis/models_live \
  grt-collector-vm:/opt/grt-reliability-tracker/data/analysis/ --zone us-east1-b

# parsed static GTFS (small; the scorer reads data/parsed_static_gtfs)
gcloud compute ssh grt-collector-vm --zone us-east1-b --command \
  "sudo -u grtcollector /opt/grt-reliability-tracker/collector/.venv/bin/python /opt/grt-reliability-tracker/collector/parse_static_gtfs.py --sync-from-gcs"

# make sure both are owned by the service user
gcloud compute ssh grt-collector-vm --zone us-east1-b --command \
  "sudo chown -R grtcollector:grtcollector /opt/grt-reliability-tracker/data"
```

### 2. Create the public live bucket (separate from the private raw bucket)

```bash
gcloud storage buckets create gs://grt-reliability-live \
  --location=us-east1 --uniform-bucket-level-access

# public read
gcloud storage buckets add-iam-policy-binding gs://grt-reliability-live \
  --member=allUsers --role=roles/storage.objectViewer

# CORS so the Pages origin can fetch it
gcloud storage buckets update gs://grt-reliability-live \
  --cors-file=ops/gcp/gcs-cors.json

# let the VM service account write to it
gcloud storage buckets add-iam-policy-binding gs://grt-reliability-live \
  --member=serviceAccount:grt-collector@grt-reliability-raw-data.iam.gserviceaccount.com \
  --role=roles/storage.objectAdmin
```

### 3. Point the VM scorer at the bucket and start it

```bash
gcloud compute ssh grt-collector-vm --zone us-east1-b --command \
  "echo 'GCS_LIVE_BUCKET=grt-reliability-live' | sudo tee -a /opt/grt-reliability-tracker/.env"

gcloud compute ssh grt-collector-vm --zone us-east1-b --command \
  "sudo systemctl enable --now grt-live-scorer.service"

gcloud compute ssh grt-collector-vm --zone us-east1-b --command \
  "journalctl -u grt-live-scorer.service -f"
```

Lag features need history, so give it ~an hour of running during GRT service hours before predictions are meaningful. Confirm the object is public and fresh:

```bash
curl -sI https://storage.googleapis.com/grt-reliability-live/live/live-predictions.json
```

### 4. Point the dashboard at the live URL

In `dashboard/index.html`, set the live URL near the top of the main `<script>`:

```js
window.GRT_CONFIG = { liveUrl: "https://storage.googleapis.com/grt-reliability-live/live/live-predictions.json" };
```

Commit and push to `main`; the Pages workflow redeploys. The panel now reads from the bucket and refreshes every minute, falling back to the bundled snapshot if the bucket is unreachable.

### Caveats

- **Service hours** — GRT only runs buses part of the day; off-hours the panel shows "not running". Expected.
- **e2-micro RAM** — the scorer runs DuckDB + LightGBM over a 90-minute window. If it OOMs on the 1 GB e2-micro (check `journalctl`), lower `--window-minutes` in `grt-live-scorer.service` or move it to a larger machine type.
- **Cost** — a 12 KB object fetched once a minute is negligible egress; the VM is the only standing cost.

## Stop Collection

```bash
gcloud compute ssh grt-collector-vm --zone us-east1-b \
  --command "sudo systemctl stop grt-collector.service"
```

## Start Collection

```bash
gcloud compute ssh grt-collector-vm --zone us-east1-b \
  --command "sudo systemctl start grt-collector.service"
```

## Delete VM

This stops VM compute charges, but keeps the GCS bucket and its data.

```bash
gcloud compute instances delete grt-collector-vm --zone us-east1-b
```
