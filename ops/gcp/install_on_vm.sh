#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/opt/grt-reliability-tracker"
COLLECTOR_SERVICE="grt-collector.service"
PARSE_SERVICE="grt-daily-parse.service"
PARSE_TIMER="grt-daily-parse.timer"
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

chmod +x "$PROJECT_DIR/ops/gcp/run_daily_parse.sh"

cp "$PROJECT_DIR/ops/gcp/$COLLECTOR_SERVICE" "/etc/systemd/system/$COLLECTOR_SERVICE"
cp "$PROJECT_DIR/ops/gcp/$PARSE_SERVICE" "/etc/systemd/system/$PARSE_SERVICE"
cp "$PROJECT_DIR/ops/gcp/$PARSE_TIMER" "/etc/systemd/system/$PARSE_TIMER"
systemctl daemon-reload
systemctl enable "$COLLECTOR_SERVICE"
systemctl restart "$COLLECTOR_SERVICE"
systemctl enable "$PARSE_TIMER"
systemctl restart "$PARSE_TIMER"

echo "Installed and started $COLLECTOR_SERVICE"
echo "Installed and started $PARSE_TIMER"
echo "Check collector logs with: journalctl -u $COLLECTOR_SERVICE -f"
echo "Check parse logs with: journalctl -u $PARSE_SERVICE -f"
