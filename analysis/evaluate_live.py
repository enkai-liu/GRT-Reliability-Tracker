"""Evaluate logged live predictions against observed delays.

predict_live.py appends every scored arrival to data/live/predictions_log.
This script closes the loop: each logged prediction is joined to the final
observed delay for the same (service_date, trip_id, stop_id, stop_sequence)
from the delay table, and the model's live error is compared against the
GTFS-RT feed's own prediction (current_predicted_delay_seconds) on exactly
the same rows.

Ground truth is the last pre-arrival observation per trip-stop, deduplicated
here regardless of how the delay table was built (--keep-all-snapshots or
not). Trip-stops whose final observation was collected more than
--max-final-lead-minutes before the predicted arrival are discarded: tracking
was lost mid-trip, so the "final" delay is itself a stale guess.

The delay table must cover the log's service dates. For dates collected on
the GCP VM, sync and build them first:

  collector/.venv/bin/python collector/parse_snapshots.py \
      --sync-from-gcs --gcs-bucket <BUCKET> --date YYYY-MM-DD --overwrite
  collector/.venv/bin/python collector/parse_static_gtfs.py \
      --sync-from-gcs --gcs-bucket <BUCKET> --date YYYY-MM-DD --overwrite
  collector/.venv/bin/python analysis/build_delay_table.py \
      --date YYYY-MM-DD --overwrite --keep-all-snapshots

Outputs under data/analysis/live_eval by default:
  - matched.parquet          — one row per logged prediction with observed delay
  - by_lead.csv              — model vs feed error by prediction lead bucket
  - by_route.csv             — per-route error summary
  - coverage_by_date.csv     — log rows vs matched rows per log date
  - evaluation_report.md     — human-readable summary (also printed)
"""

import argparse
from datetime import date, timedelta
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG_ROOT = PROJECT_ROOT / "data" / "live" / "predictions_log"
DEFAULT_DELAY_ROOT = PROJECT_ROOT / "data" / "analysis" / "delay_table"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data" / "analysis" / "live_eval"

LEAD_BUCKETS = ((0, 5), (5, 10), (10, 20), (20, 40), (40, None))


def iso_date(value):
    return date.fromisoformat(value).isoformat()


def sql_list(values):
    return ", ".join(f"'{value}'" for value in values)


def lead_bucket_sql(column):
    parts = []
    for low, high in LEAD_BUCKETS:
        if high is None:
            parts.append(f"WHEN {column} >= {low} THEN '{low}+ min'")
        else:
            parts.append(f"WHEN {column} >= {low} AND {column} < {high} THEN '{low}-{high} min'")
    return "CASE " + " ".join(parts) + " END"


def load_predictions_log(con, log_root, dates):
    date_filter = ""
    if dates:
        date_filter = f"WHERE date IN ({sql_list(dates)})"
    con.sql(f"""
        CREATE OR REPLACE VIEW log_raw AS
        SELECT * FROM read_parquet(
            '{log_root}/date=*/run-*.parquet',
            hive_partitioning = true,
            union_by_name = true
        )
        {date_filter}
    """)

    # Logs written before quantile models existed lack the interval columns.
    columns = {row[0] for row in con.sql("DESCRIBE log_raw").fetchall()}
    interval_cols = {"predicted_delay_lower_seconds", "predicted_delay_upper_seconds"}
    has_intervals = interval_cols <= columns
    lower = "predicted_delay_lower_seconds" if has_intervals else "NULL::DOUBLE AS predicted_delay_lower_seconds"
    upper = "predicted_delay_upper_seconds" if has_intervals else "NULL::DOUBLE AS predicted_delay_upper_seconds"

    con.sql(f"""
        CREATE OR REPLACE VIEW log AS
        SELECT
            date AS log_date,
            collected_at_utc,
            transit_mode,
            trip_id,
            route_id,
            stop_id,
            stop_sequence,
            service_date,
            scheduled_arrival_utc,
            prediction_lead_minutes,
            current_predicted_delay_seconds,
            predicted_delay_seconds,
            {lower},
            {upper}
        FROM log_raw
        WHERE prediction_lead_minutes >= 0
    """)
    return has_intervals


def needed_delay_dates(con):
    """Delay table partitions that may hold ground truth: every service date in
    the log plus the following calendar day (post-midnight arrivals land in the
    next snapshot_date partition)."""
    service_dates = [row[0] for row in con.sql("SELECT DISTINCT service_date FROM log").fetchall()]
    needed = set()
    for d in service_dates:
        needed.add(d.isoformat())
        needed.add((d + timedelta(days=1)).isoformat())
    return sorted(needed)


def create_observed_view(con, delay_root, delay_dates, max_final_lead_minutes):
    available = sorted(
        entry.name.removeprefix("date=")
        for entry in Path(delay_root).iterdir()
        if entry.is_dir() and entry.name.startswith("date=")
        and (entry / "part-000.parquet").exists()
    )
    usable = [d for d in delay_dates if d in available]
    missing = [d for d in delay_dates if d not in available]
    if not usable:
        raise SystemExit(
            f"No delay table partitions for the log's service dates under {delay_root}.\n"
            f"Needed: {', '.join(delay_dates)}\n"
            f"Build them first (see this script's docstring)."
        )

    con.sql(f"""
        CREATE OR REPLACE VIEW observed AS
        SELECT * EXCLUDE (rn)
        FROM (
            SELECT
                service_date,
                trip_id,
                stop_id,
                stop_sequence,
                delay_seconds AS observed_delay_seconds,
                collected_at_utc AS observed_collected_at_utc,
                prediction_lead_minutes AS final_lead_minutes,
                ROW_NUMBER() OVER (
                    PARTITION BY service_date, trip_id, stop_id, stop_sequence
                    ORDER BY collected_at_utc DESC
                ) AS rn
            FROM read_parquet('{delay_root}/date=*/part-000.parquet', hive_partitioning = true)
            WHERE date IN ({sql_list(usable)})
              AND prediction_lead_minutes >= 0
        )
        WHERE rn = 1
          AND final_lead_minutes <= {max_final_lead_minutes}
    """)
    return usable, missing


def create_matched_table(con):
    con.sql("""
        CREATE OR REPLACE TABLE matched AS
        SELECT
            l.*,
            o.observed_delay_seconds,
            o.final_lead_minutes,
            l.predicted_delay_seconds - o.observed_delay_seconds AS model_error_seconds,
            l.current_predicted_delay_seconds - o.observed_delay_seconds AS feed_error_seconds
        FROM log l
        JOIN observed o
            USING (service_date, trip_id, stop_id, stop_sequence)
    """)


def error_summary(con, group_sql=None, group_label=None, having=""):
    """Model-vs-feed error stats, optionally grouped."""
    group_cols = ""
    group_by = ""
    if group_sql:
        group_cols = f"{group_sql} AS {group_label},"
        group_by = f"GROUP BY {group_sql}"
    return con.sql(f"""
        SELECT
            {group_cols}
            count(*) AS rows,
            round(avg(abs(model_error_seconds)), 1) AS model_mae_s,
            round(avg(abs(feed_error_seconds)), 1) AS feed_mae_s,
            round(median(abs(model_error_seconds)), 1) AS model_median_ae_s,
            round(median(abs(feed_error_seconds)), 1) AS feed_median_ae_s,
            round(sqrt(avg(model_error_seconds * model_error_seconds)), 1) AS model_rmse_s,
            round(sqrt(avg(feed_error_seconds * feed_error_seconds)), 1) AS feed_rmse_s,
            round(avg(model_error_seconds), 1) AS model_bias_s,
            round(avg(feed_error_seconds), 1) AS feed_bias_s
        FROM matched
        {group_by}
        {having}
    """).fetchall()


def late_detection(con, threshold):
    return con.sql(f"""
        SELECT
            sum((observed_delay_seconds > {threshold})::INT) AS observed_late,
            sum((predicted_delay_seconds > {threshold})::INT) AS model_flagged,
            sum((predicted_delay_seconds > {threshold} AND observed_delay_seconds > {threshold})::INT) AS model_tp,
            sum((current_predicted_delay_seconds > {threshold})::INT) AS feed_flagged,
            sum((current_predicted_delay_seconds > {threshold} AND observed_delay_seconds > {threshold})::INT) AS feed_tp
        FROM matched
    """).fetchone()


def interval_summary(con, group_sql=None, group_label=None):
    group_cols = ""
    group_by = ""
    if group_sql:
        group_cols = f"{group_sql} AS {group_label},"
        group_by = f"GROUP BY {group_sql}"
    return con.sql(f"""
        SELECT
            {group_cols}
            count(*) AS rows,
            round(avg((observed_delay_seconds
                       BETWEEN predicted_delay_lower_seconds
                       AND predicted_delay_upper_seconds)::INT), 3) AS coverage,
            round(avg((observed_delay_seconds < predicted_delay_lower_seconds)::INT), 3) AS below_lower,
            round(avg((observed_delay_seconds > predicted_delay_upper_seconds)::INT), 3) AS above_upper,
            round(avg(predicted_delay_upper_seconds - predicted_delay_lower_seconds), 1) AS mean_width_s
        FROM matched
        WHERE predicted_delay_lower_seconds IS NOT NULL
          AND predicted_delay_upper_seconds IS NOT NULL
        {group_by}
        ORDER BY 1
    """).fetchall()


def markdown_table(headers, rows):
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join("" if v is None else str(v) for v in row) + " |")
    return "\n".join(lines)


def precision_recall(flagged, tp, actual):
    precision = tp / flagged if flagged else None
    recall = tp / actual if actual else None
    fmt = lambda v: f"{v:.2f}" if v is not None else "n/a"
    return fmt(precision), fmt(recall)


def lead_bucket_order(label):
    return int(label.split("-")[0].rstrip("+ min").strip())


def build_report(con, args, has_intervals, usable_dates, missing_dates, late_threshold):
    lines = ["# Live Prediction Evaluation", ""]

    log_total = con.sql("SELECT count(*) FROM log").fetchone()[0]
    matched_total = con.sql("SELECT count(*) FROM matched").fetchone()[0]
    span = con.sql("SELECT min(collected_at_utc), max(collected_at_utc) FROM matched").fetchone()
    trips = con.sql("SELECT count(DISTINCT trip_id) FROM matched").fetchone()[0]

    lines.append(f"- Logged predictions: **{log_total}**, matched to an observed arrival: "
                 f"**{matched_total}** ({matched_total / log_total:.0%})")
    if span[0] is not None:
        lines.append(f"- Prediction window: {span[0]:%Y-%m-%d %H:%M} to {span[1]:%Y-%m-%d %H:%M} UTC, "
                     f"{trips} distinct trips")
    lines.append(f"- Ground truth: final pre-arrival observation per trip-stop "
                 f"(final lead <= {args.max_final_lead_minutes:g} min), "
                 f"delay table dates: {', '.join(usable_dates)}")
    if missing_dates:
        lines.append(f"- **Missing delay table dates** (predictions there are unmatched): "
                     f"{', '.join(missing_dates)}")
    lines.append("")

    coverage_rows = con.sql("""
        SELECT l.log_date, count(*) AS log_rows, count(m.observed_delay_seconds) AS matched_rows,
               round(count(m.observed_delay_seconds) / count(*)::DOUBLE, 3) AS match_rate
        FROM log l
        LEFT JOIN matched m
            USING (service_date, trip_id, stop_id, stop_sequence, collected_at_utc)
        GROUP BY l.log_date ORDER BY l.log_date
    """).fetchall()

    lines.append("## Coverage by log date")
    lines.append("")
    lines.append(markdown_table(["log date", "log rows", "matched", "match rate"], coverage_rows))
    lines.append("")

    headers = ["rows", "model MAE (s)", "feed MAE (s)", "model med AE", "feed med AE",
               "model RMSE", "feed RMSE", "model bias", "feed bias"]

    lines.append("## Overall error — model vs GTFS-RT feed")
    lines.append("")
    lines.append(markdown_table(headers, error_summary(con)))
    lines.append("")

    bucket_sql = lead_bucket_sql("prediction_lead_minutes")
    by_lead = sorted(error_summary(con, bucket_sql, "lead_bucket"),
                     key=lambda row: lead_bucket_order(row[0]))
    lines.append("## By prediction lead time")
    lines.append("")
    lines.append(markdown_table(["lead"] + headers, by_lead))
    lines.append("")

    by_mode = error_summary(con, "transit_mode", "transit_mode")
    lines.append("## By mode")
    lines.append("")
    lines.append(markdown_table(["mode"] + headers, sorted(by_mode)))
    lines.append("")

    observed_late, model_flagged, model_tp, feed_flagged, feed_tp = late_detection(con, late_threshold)
    model_p, model_r = precision_recall(model_flagged, model_tp, observed_late)
    feed_p, feed_r = precision_recall(feed_flagged, feed_tp, observed_late)
    lines.append(f"## Late-arrival detection (> {late_threshold}s observed)")
    lines.append("")
    lines.append(markdown_table(
        ["", "flagged", "true positives", "precision", "recall"],
        [
            ("model", model_flagged, model_tp, model_p, model_r),
            ("feed", feed_flagged, feed_tp, feed_p, feed_r),
            ("observed late rows", observed_late, "", "", ""),
        ],
    ))
    lines.append("")

    lines.append("## Prediction intervals")
    lines.append("")
    interval_rows = interval_summary(con) if has_intervals else []
    if interval_rows and interval_rows[0][0] > 0:
        lines.append(markdown_table(
            ["rows", "coverage", "below lower", "above upper", "mean width (s)"],
            interval_rows,
        ))
        by_lead_interval = sorted(interval_summary(con, bucket_sql, "lead_bucket"),
                                  key=lambda row: lead_bucket_order(row[0]))
        lines.append("")
        lines.append(markdown_table(
            ["lead", "rows", "coverage", "below lower", "above upper", "mean width (s)"],
            by_lead_interval,
        ))
        nominal = "Target coverage for a q10-q90 interval is 0.80."
        lines.append("")
        lines.append(nominal)
    else:
        lines.append("No logged predictions carry interval columns yet "
                     "(retrain with quantiles and rerun predict_live.py).")
    lines.append("")

    by_route = error_summary(
        con, "transit_mode || ' ' || route_id", "route",
        having=f"HAVING count(*) >= {args.min_route_rows}",
    )
    by_route.sort(key=lambda row: -row[2])
    lines.append(f"## Routes with most live model error (>= {args.min_route_rows} matched rows)")
    lines.append("")
    lines.append(markdown_table(["route"] + headers, by_route[:15]))
    lines.append("")

    return "\n".join(lines)


def write_outputs(con, args, report):
    output_root = args.output_root
    output_root.mkdir(parents=True, exist_ok=True)

    con.sql(f"""
        COPY (SELECT * FROM matched ORDER BY collected_at_utc, trip_id, stop_sequence)
        TO '{output_root / "matched.parquet"}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)

    bucket_sql = lead_bucket_sql("prediction_lead_minutes")
    summaries = {
        "by_lead.csv": f"""
            SELECT {bucket_sql} AS lead_bucket,
                   count(*) AS rows,
                   round(avg(abs(model_error_seconds)), 1) AS model_mae_s,
                   round(avg(abs(feed_error_seconds)), 1) AS feed_mae_s,
                   round(avg(model_error_seconds), 1) AS model_bias_s,
                   round(avg(feed_error_seconds), 1) AS feed_bias_s
            FROM matched GROUP BY 1 ORDER BY min(prediction_lead_minutes)
        """,
        "by_route.csv": """
            SELECT transit_mode, route_id,
                   count(*) AS rows,
                   round(avg(abs(model_error_seconds)), 1) AS model_mae_s,
                   round(avg(abs(feed_error_seconds)), 1) AS feed_mae_s
            FROM matched GROUP BY 1, 2 ORDER BY model_mae_s DESC
        """,
        "coverage_by_date.csv": """
            SELECT l.log_date, count(*) AS log_rows,
                   count(m.observed_delay_seconds) AS matched_rows
            FROM log l
            LEFT JOIN matched m
                USING (service_date, trip_id, stop_id, stop_sequence, collected_at_utc)
            GROUP BY l.log_date ORDER BY l.log_date
        """,
    }
    for filename, query in summaries.items():
        con.sql(f"COPY ({query}) TO '{output_root / filename}' (HEADER, DELIMITER ',')")

    report_path = output_root / "evaluation_report.md"
    report_path.write_text(report, encoding="utf-8")
    return report_path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Evaluate logged live predictions against observed delays."
    )
    parser.add_argument("--log-root", type=Path, default=DEFAULT_LOG_ROOT,
                        help="Root of predict_live.py per-run Parquet logs.")
    parser.add_argument("--delay-root", type=Path, default=DEFAULT_DELAY_ROOT,
                        help="Root of the delay table used as ground truth.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT,
                        help="Output directory for evaluation artifacts.")
    parser.add_argument("--date", action="append", type=iso_date,
                        help="Log date (YYYY-MM-DD) to evaluate. May be repeated. "
                             "Defaults to all logged dates.")
    parser.add_argument("--max-final-lead-minutes", type=float, default=2.0,
                        help="Ground-truth observations must be within this many minutes "
                             "of the predicted arrival; otherwise tracking was lost.")
    parser.add_argument("--late-delay-threshold-seconds", type=int, default=300,
                        help="Observed delay above this counts as a late arrival.")
    parser.add_argument("--min-route-rows", type=int, default=200,
                        help="Minimum matched rows for a route to appear in the route table.")
    return parser.parse_args()


def main():
    args = parse_args()
    args.log_root = args.log_root.resolve()
    args.delay_root = args.delay_root.resolve()
    args.output_root = args.output_root.resolve()

    if not args.log_root.exists():
        raise SystemExit(f"No predictions log found at {args.log_root}. Run predict_live.py first.")

    con = duckdb.connect()
    con.sql("SET timezone = 'UTC'")

    has_intervals = load_predictions_log(con, args.log_root, args.date)
    delay_dates = needed_delay_dates(con)
    usable_dates, missing_dates = create_observed_view(
        con, args.delay_root, delay_dates, args.max_final_lead_minutes
    )
    create_matched_table(con)

    matched = con.sql("SELECT count(*) FROM matched").fetchone()[0]
    if matched == 0:
        raise SystemExit(
            "No logged predictions matched an observed arrival. "
            f"Delay table dates used: {', '.join(usable_dates)}; "
            f"missing: {', '.join(missing_dates) or 'none'}."
        )

    report = build_report(
        con, args, has_intervals, usable_dates, missing_dates,
        args.late_delay_threshold_seconds,
    )
    report_path = write_outputs(con, args, report)

    print(report)
    print(f"Wrote {report_path}")


if __name__ == "__main__":
    main()
