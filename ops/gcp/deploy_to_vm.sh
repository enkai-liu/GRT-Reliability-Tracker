#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-grt-reliability-raw-data}"
VM_NAME="${VM_NAME:-grt-collector-vm}"
ZONE="${ZONE:-us-east1-b}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-micro}"
BUCKET_NAME="${BUCKET_NAME:-grt-reliability-raw-data}"
SERVICE_ACCOUNT_NAME="${SERVICE_ACCOUNT_NAME:-grt-collector}"
SERVICE_ACCOUNT_EMAIL="$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ARCHIVE="/tmp/grt-reliability-tracker-vm.tar.gz"

cd "$PROJECT_ROOT"

gcloud config set project "$PROJECT_ID"
gcloud services enable compute.googleapis.com iam.googleapis.com storage.googleapis.com

if ! gcloud iam service-accounts describe "$SERVICE_ACCOUNT_EMAIL" >/dev/null 2>&1; then
  gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
    --display-name="GRT collector VM service account"
fi

gcloud storage buckets add-iam-policy-binding "gs://$BUCKET_NAME" \
  --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
  --role="roles/storage.objectAdmin"

if ! gcloud compute instances describe "$VM_NAME" --zone "$ZONE" >/dev/null 2>&1; then
  gcloud compute instances create "$VM_NAME" \
    --zone "$ZONE" \
    --machine-type "$MACHINE_TYPE" \
    --image-family debian-12 \
    --image-project debian-cloud \
    --boot-disk-size 20GB \
    --boot-disk-type pd-standard \
    --service-account "$SERVICE_ACCOUNT_EMAIL" \
    --scopes https://www.googleapis.com/auth/cloud-platform \
    --metadata enable-oslogin=FALSE
fi

tar \
  --exclude="./.env" \
  --exclude="./.env.*" \
  --exclude="./*.json" \
  --exclude="./*credentials*" \
  --exclude="./*service-account*" \
  --exclude="./collector/.venv" \
  --exclude="./data" \
  --exclude="./logs" \
  --exclude="./.git" \
  --exclude="./__pycache__" \
  --exclude="./collector/__pycache__" \
  -czf "$ARCHIVE" .

gcloud compute ssh "$VM_NAME" --zone "$ZONE" --command "sudo systemctl stop grt-collector.service >/dev/null 2>&1 || true"
gcloud compute ssh "$VM_NAME" --zone "$ZONE" --command "sudo rm -rf /opt/grt-reliability-tracker && sudo mkdir -p /opt/grt-reliability-tracker && sudo chown \$(whoami) /opt/grt-reliability-tracker"
gcloud compute scp "$ARCHIVE" "$VM_NAME:/tmp/grt-reliability-tracker-vm.tar.gz" --zone "$ZONE"
gcloud compute ssh "$VM_NAME" --zone "$ZONE" --command "tar -xzf /tmp/grt-reliability-tracker-vm.tar.gz -C /opt/grt-reliability-tracker"
gcloud compute ssh "$VM_NAME" --zone "$ZONE" --command "printf '%s\n' 'POLL_SECONDS=30' 'ALERT_POLL_SECONDS=300' 'DATA_ROOT=data' 'GCS_BUCKET=$BUCKET_NAME' 'WEATHER_FORECAST_LOCATION_ID=on-82' 'WEATHER_FORECAST_LOCATION_NAME=kitchener_waterloo' 'WEATHER_FORECAST_API_URL=https://api.weather.gc.ca/collections/citypageweather-realtime/items' > /opt/grt-reliability-tracker/.env"
gcloud compute ssh "$VM_NAME" --zone "$ZONE" --command "sudo bash /opt/grt-reliability-tracker/ops/gcp/install_on_vm.sh"

echo "Deployment complete."
echo "Check status:"
echo "  gcloud compute ssh $VM_NAME --zone $ZONE --command 'systemctl status grt-collector.service --no-pager'"
echo "Watch logs:"
echo "  gcloud compute ssh $VM_NAME --zone $ZONE --command 'journalctl -u grt-collector.service -f'"
