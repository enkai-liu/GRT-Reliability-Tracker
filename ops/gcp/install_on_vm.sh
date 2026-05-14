#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/opt/grt-reliability-tracker"
SERVICE_NAME="grt-collector.service"
SERVICE_USER="grtcollector"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run this script with sudo."
  exit 1
fi

if ! id "$SERVICE_USER" >/dev/null 2>&1; then
  useradd --system --create-home --shell /usr/sbin/nologin "$SERVICE_USER"
fi

apt-get update
apt-get install -y python3 python3-venv python3-pip ca-certificates

mkdir -p "$PROJECT_DIR/logs"
chown -R "$SERVICE_USER:$SERVICE_USER" "$PROJECT_DIR"

python3 -m venv "$PROJECT_DIR/collector/.venv"
"$PROJECT_DIR/collector/.venv/bin/python" -m pip install --upgrade pip
"$PROJECT_DIR/collector/.venv/bin/python" -m pip install -r "$PROJECT_DIR/collector/requirements.txt"

cp "$PROJECT_DIR/ops/gcp/$SERVICE_NAME" "/etc/systemd/system/$SERVICE_NAME"
systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"

echo "Installed and started $SERVICE_NAME"
echo "Check logs with: journalctl -u $SERVICE_NAME -f"
