#!/usr/bin/env bash
set -euo pipefail

LABEL="com.enkailiu.grt-reliability.collector"
PROJECT_ROOT="/Users/enkailiu/grt-reliability-tracker"
SOURCE_PLIST="$PROJECT_ROOT/ops/launchd/$LABEL.plist"
TARGET_DIR="$HOME/Library/LaunchAgents"
TARGET_PLIST="$TARGET_DIR/$LABEL.plist"

mkdir -p "$PROJECT_ROOT/logs"
mkdir -p "$TARGET_DIR"

if launchctl print "gui/$(id -u)/$LABEL" >/dev/null 2>&1; then
  launchctl bootout "gui/$(id -u)" "$TARGET_PLIST"
fi

cp "$SOURCE_PLIST" "$TARGET_PLIST"
launchctl bootstrap "gui/$(id -u)" "$TARGET_PLIST"
launchctl enable "gui/$(id -u)/$LABEL"
launchctl kickstart -k "gui/$(id -u)/$LABEL"

echo "Installed and started $LABEL"
echo "Logs:"
echo "  $PROJECT_ROOT/logs/collector.out.log"
echo "  $PROJECT_ROOT/logs/collector.err.log"
