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
static_gtfs/
  bus_static_gtfs/YYYY-MM-DD/GTFS.zip
  lrt_static_gtfs/YYYY-MM-DD/GTFS.zip
```
