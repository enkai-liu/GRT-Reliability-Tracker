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

The VM collects ECCC Kitchener-Waterloo forecast snapshots every 3 hours:

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
