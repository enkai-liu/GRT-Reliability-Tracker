# Data Directory

This directory is for local collector output and downloaded analysis samples.

Raw files are intentionally ignored by Git. Keep canonical long-term data in Google Cloud Storage.

Expected local layout:

```text
raw/
  bus_trip_updates/YYYY-MM-DD/timestamp.pb
  bus_vehicle_positions/YYYY-MM-DD/timestamp.pb
  lrt_trip_updates/YYYY-MM-DD/timestamp.pb
  lrt_vehicle_positions/YYYY-MM-DD/timestamp.pb
  grt_service_alerts/YYYY-MM-DD/timestamp.pb
  weather_forecasts/kitchener_waterloo/YYYY-MM-DD/timestamp.json
  weather_forecasts/cambridge/YYYY-MM-DD/timestamp.json
static_gtfs/
  bus_static_gtfs/YYYY-MM-DD/GTFS.zip
  lrt_static_gtfs/YYYY-MM-DD/GTFS.zip
parsed/
  feed_snapshots/date=YYYY-MM-DD/part-000.parquet
  vehicle_positions/date=YYYY-MM-DD/part-000.parquet
  trip_updates/date=YYYY-MM-DD/part-000.parquet
  stop_time_updates/date=YYYY-MM-DD/part-000.parquet
  service_alerts/date=YYYY-MM-DD/part-000.parquet
  service_alert_active_periods/date=YYYY-MM-DD/part-000.parquet
  service_alert_informed_entities/date=YYYY-MM-DD/part-000.parquet
parsed_static_gtfs/
  routes/p_feed=bus_static_gtfs/p_snapshot_date=YYYY-MM-DD/part-000.parquet
  stops/p_feed=bus_static_gtfs/p_snapshot_date=YYYY-MM-DD/part-000.parquet
  trips/p_feed=bus_static_gtfs/p_snapshot_date=YYYY-MM-DD/part-000.parquet
  stop_times/p_feed=bus_static_gtfs/p_snapshot_date=YYYY-MM-DD/part-000.parquet
```
