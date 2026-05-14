# LaunchAgent

Use this LaunchAgent to run the GRT collector in the background on macOS without keeping a terminal open.

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
