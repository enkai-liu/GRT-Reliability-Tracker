# LaunchAgent

Use this LaunchAgent to run the GRT collector in the background on macOS without keeping a terminal open.

For production collection, the GCP VM is preferred. Local LaunchAgents only run while this Mac is awake and logged in.

## Install and start

```bash
chmod +x ops/launchd/install_collector_launch_agent.sh
chmod +x ops/launchd/uninstall_collector_launch_agent.sh
ops/launchd/install_collector_launch_agent.sh
```

The collector starts immediately and also starts again when you log in.

## Check status

```bash
launchctl print gui/$(id -u)/com.enkailiu.grt-reliability.collector
```

## Watch logs

```bash
tail -f logs/collector.out.log
tail -f logs/collector.err.log
```

## Stop and remove

```bash
ops/launchd/uninstall_collector_launch_agent.sh
```

## Restart after code changes

```bash
launchctl kickstart -k gui/$(id -u)/com.enkailiu.grt-reliability.collector
```

## Daily local parsing

Parsing is safer to run locally than on the small collector VM. The helper script parses yesterday by default:

```bash
collector/.venv/bin/python collector/run_local_parse.py
```

Catch up a range after being away:

```bash
collector/.venv/bin/python collector/run_local_parse.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD
```

Optionally install a daily LaunchAgent that tries to parse yesterday at 05:30 local time:

```bash
chmod +x ops/launchd/install_daily_parse_launch_agent.sh
chmod +x ops/launchd/uninstall_daily_parse_launch_agent.sh
ops/launchd/install_daily_parse_launch_agent.sh
```

Check status:

```bash
launchctl print gui/$(id -u)/com.enkailiu.grt-reliability.daily-parse
```

Watch logs:

```bash
tail -f logs/daily-parse.out.log
tail -f logs/daily-parse.err.log
```

Remove it:

```bash
ops/launchd/uninstall_daily_parse_launch_agent.sh
```
