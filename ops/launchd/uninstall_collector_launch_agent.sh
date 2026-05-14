#!/usr/bin/env bash
set -euo pipefail

LABEL="com.enkailiu.grt-reliability.collector"
TARGET_PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"

if launchctl print "gui/$(id -u)/$LABEL" >/dev/null 2>&1; then
  launchctl bootout "gui/$(id -u)" "$TARGET_PLIST"
fi

rm -f "$TARGET_PLIST"

echo "Uninstalled $LABEL"
