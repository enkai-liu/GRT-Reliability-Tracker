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
