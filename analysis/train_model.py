"""Train delay prediction model from the enriched feature table.

Evaluates baseline models and a LightGBM model, saves the best performer.

Baselines:
  0. Schedule (predict delay = 0)
  1. Route-hour average (median delay per route_id, hour_of_day, is_weekend)
  2. Route-stop-hour average (median delay per route_id, stop_id, hour_of_day, is_weekend)

Primary model:
  LightGBM gradient boosted trees with MAE objective, plus quantile-objective
  models (default q10/q90) for prediction intervals ("arrives +2 to +9 min").

Output:
  - data/analysis/models/lgbm_model.txt (LightGBM point model)
  - data/analysis/models/lgbm_model_q10.txt, lgbm_model_q90.txt (quantile models)
  - data/analysis/models/route_hour_avg.parquet (baseline lookup)
  - data/analysis/models/route_stop_hour_avg.parquet (baseline lookup)
  - data/analysis/models/evaluation.txt (comparison report)
"""

import argparse
import json
from pathlib import Path

import duckdb
import lightgbm as lgb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FEATURES_ROOT = PROJECT_ROOT / "data" / "analysis" / "features"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data" / "analysis" / "models"

# Features for LightGBM
NUMERIC_FEATURES = [
    "current_predicted_delay_seconds",
    "stop_sequence_normalized",
    "hour_of_day",
    "minute_of_hour",
    "day_of_week",
    "temperature_c",
    "wind_speed_kmh",
    "precip_probability_pct",
    "prediction_lead_minutes",
    "prior_prediction_count",
    "previous_predicted_delay_seconds",
    "previous_2_predicted_delay_seconds",
    "previous_5_predicted_delay_seconds",
    "predicted_delay_delta_seconds",
    "minutes_since_previous_prediction",
    "recent_predicted_delay_mean_5",
    "recent_predicted_delay_min_5",
    "recent_predicted_delay_max_5",
    "previous_stop_predicted_delay_seconds",
    "previous_stop_predicted_delay_delta_seconds",
    "vehicle_speed",
    "vehicle_current_stop_sequence",
    "vehicle_current_status",
    "vehicle_update_age_seconds",
    "vehicle_distance_to_stop_m",
    "vehicle_stop_sequence_delta",
    "route_type",
]

BOOLEAN_FEATURES = [
    "is_weekend",
    "is_rush_hour",
    "is_rain",
    "is_snow",
    "is_precip",
    "is_vehicle_update_stale",
]

CATEGORICAL_FEATURES = [
    "route_id",
    "stop_id",
    "direction_id",
    "transit_mode",
]

ALL_FEATURES = NUMERIC_FEATURES + BOOLEAN_FEATURES + CATEGORICAL_FEATURES
TARGET = "delay_seconds"


def load_features(con, features_root):
    """Load feature table into DuckDB."""
    con.sql(f"""
        CREATE TABLE features AS
        SELECT * FROM read_parquet('{features_root}/date=*/part-000.parquet',
                                    hive_partitioning=true)
        WHERE delay_seconds IS NOT NULL
          AND abs(delay_seconds) < 3600  -- exclude extreme outliers (>1 hour)
    """)
    count = con.sql("SELECT count(*) FROM features").fetchone()[0]
    dates = con.sql("SELECT count(DISTINCT snapshot_date) FROM features").fetchone()[0]
    print(f"Loaded {count} rows across {dates} dates")
    return count, dates


def get_time_split(con):
    """Return train and validation dates using the same time split for all models."""
    dates = [row[0] for row in con.sql(
        "SELECT DISTINCT snapshot_date FROM features ORDER BY snapshot_date"
    ).fetchall()]

    if len(dates) < 2:
        print("WARNING: Only 1 date available. Using full dataset for training and validation.")
        return dates, dates

    split_idx = max(1, int(len(dates) * 0.8))
    return dates[:split_idx], dates[split_idx:]


def date_filter_sql(date_list):
    return ", ".join(f"'{d}'" for d in date_list)


def build_baselines(con, output_root, train_dates):
    """Compute and save baseline lookup tables from training dates only."""
    train_filter = date_filter_sql(train_dates)

    # Baseline 1: route-hour average
    con.sql(f"""
        CREATE TABLE route_hour_avg AS
        SELECT
            route_id,
            hour_of_day,
            is_weekend,
            median(delay_seconds) AS median_delay,
            avg(delay_seconds) AS mean_delay,
            count(*) AS sample_count
        FROM features
        WHERE snapshot_date IN ({train_filter})
        GROUP BY route_id, hour_of_day, is_weekend
    """)
    con.sql(f"COPY route_hour_avg TO '{output_root}/route_hour_avg.parquet' (FORMAT PARQUET)")
    rha_count = con.sql("SELECT count(*) FROM route_hour_avg").fetchone()[0]
    print(f"Baseline 1 (route-hour avg): {rha_count} lookup entries")

    # Baseline 2: route-stop-hour average
    con.sql(f"""
        CREATE TABLE route_stop_hour_avg AS
        SELECT
            route_id,
            stop_id,
            hour_of_day,
            is_weekend,
            median(delay_seconds) AS median_delay,
            avg(delay_seconds) AS mean_delay,
            count(*) AS sample_count
        FROM features
        WHERE snapshot_date IN ({train_filter})
        GROUP BY route_id, stop_id, hour_of_day, is_weekend
    """)
    con.sql(f"COPY route_stop_hour_avg TO '{output_root}/route_stop_hour_avg.parquet' (FORMAT PARQUET)")
    rsha_count = con.sql("SELECT count(*) FROM route_stop_hour_avg").fetchone()[0]
    print(f"Baseline 2 (route-stop-hour avg): {rsha_count} lookup entries")


def evaluate_baselines(con, val_dates):
    """Compute validation MAE for each baseline."""
    results = {}
    val_filter = date_filter_sql(val_dates)

    # Baseline 0: schedule (predict 0)
    mae_0 = con.sql(f"""
        SELECT avg(abs(delay_seconds))
        FROM features
        WHERE snapshot_date IN ({val_filter})
    """).fetchone()[0]
    results["schedule (delay=0)"] = mae_0

    # Baseline 0b: use the current GTFS-RT predicted delay directly. This is
    # the model's main practical benchmark when training on all snapshots.
    raw_prediction_count = con.sql("""
        SELECT count(*)
        FROM information_schema.columns
        WHERE table_name = 'features'
          AND column_name = 'current_predicted_delay_seconds'
    """).fetchone()[0]
    if raw_prediction_count:
        mae_raw = con.sql(f"""
            SELECT avg(abs(delay_seconds - current_predicted_delay_seconds))
            FROM features
            WHERE snapshot_date IN ({val_filter})
        """).fetchone()[0]
        results["raw GTFS-RT prediction"] = mae_raw

    # Baseline 1: route-hour average
    mae_1 = con.sql(f"""
        SELECT avg(abs(f.delay_seconds - COALESCE(b.median_delay, 0)))
        FROM features f
        LEFT JOIN route_hour_avg b
            ON f.route_id = b.route_id
            AND f.hour_of_day = b.hour_of_day
            AND f.is_weekend = b.is_weekend
        WHERE f.snapshot_date IN ({val_filter})
    """).fetchone()[0]
    results["route-hour avg"] = mae_1

    # Baseline 2: route-stop-hour average. Fall back to route-hour where a
    # route/stop/hour bucket was not observed in the training window.
    mae_2 = con.sql(f"""
        SELECT avg(abs(f.delay_seconds - COALESCE(b.median_delay, rh.median_delay, 0)))
        FROM features f
        LEFT JOIN route_stop_hour_avg b
            ON f.route_id = b.route_id
            AND f.stop_id = b.stop_id
            AND f.hour_of_day = b.hour_of_day
            AND f.is_weekend = b.is_weekend
        LEFT JOIN route_hour_avg rh
            ON f.route_id = rh.route_id
            AND f.hour_of_day = rh.hour_of_day
            AND f.is_weekend = rh.is_weekend
        WHERE f.snapshot_date IN ({val_filter})
    """).fetchone()[0]
    results["route-stop-hour avg"] = mae_2

    return results


def prepare_lgbm_data(con):
    """Extract feature matrix and target for LightGBM."""
    # Convert booleans to int and categoricals to codes in DuckDB
    select_parts = []
    for f in NUMERIC_FEATURES:
        select_parts.append(f"COALESCE({f}, 0) AS {f}")
    for f in BOOLEAN_FEATURES:
        select_parts.append(f"CAST(COALESCE({f}, false) AS INTEGER) AS {f}")
    for f in CATEGORICAL_FEATURES:
        select_parts.append(f"CAST({f} AS VARCHAR) AS {f}")

    select_sql = ", ".join(select_parts)

    result = con.sql(f"""
        SELECT {select_sql}, {TARGET}, snapshot_date
        FROM features
    """).fetchall()

    columns = [f for f in ALL_FEATURES] + [TARGET, "snapshot_date"]
    return result, columns


def quantile_model_filename(quantile):
    return f"lgbm_model_q{int(round(quantile * 100)):02d}.txt"


def train_lgbm(
    con,
    output_root,
    train_dates,
    val_dates,
    max_train_rows,
    max_val_rows,
    late_delay_threshold_seconds,
    late_delay_weight,
    quantiles,
):
    """Train LightGBM model with time-based split."""
    import numpy as np

    print(f"Train dates: {train_dates[0]}..{train_dates[-1]} ({len(train_dates)} days)")
    print(f"Val dates:   {val_dates[0]}..{val_dates[-1]} ({len(val_dates)} days)")

    # Extract arrays
    feature_select = []
    for f in NUMERIC_FEATURES:
        feature_select.append(f"COALESCE({f}, 0)::DOUBLE AS {f}")
    for f in BOOLEAN_FEATURES:
        feature_select.append(f"CAST(COALESCE({f}, false) AS DOUBLE) AS {f}")
    for f in CATEGORICAL_FEATURES:
        # LightGBM expects categorical features as integer codes.
        feature_select.append(
            f"CAST(hash(COALESCE(CAST({f} AS VARCHAR), '')) % 100000 AS INTEGER) AS {f}"
        )

    select_sql = ", ".join(feature_select)

    def fetch_split(date_list, max_rows):
        dl = ", ".join(f"'{d}'" for d in date_list)
        limit_sql = ""
        order_sql = ""
        if max_rows:
            order_sql = """
            ORDER BY hash(
                COALESCE(CAST(snapshot_date AS VARCHAR), ''),
                COALESCE(CAST(trip_id AS VARCHAR), ''),
                COALESCE(CAST(stop_id AS VARCHAR), ''),
                COALESCE(CAST(collected_at_utc AS VARCHAR), '')
            )
            """
            limit_sql = f"LIMIT {max_rows}"

        rows = con.sql(f"""
            SELECT
                {select_sql},
                {TARGET},
                CASE
                    WHEN {TARGET} > {late_delay_threshold_seconds}
                    THEN {late_delay_weight}
                    ELSE 1.0
                END AS sample_weight
            FROM features
            WHERE snapshot_date IN ({dl})
            {order_sql}
            {limit_sql}
        """).fetchall()

        n_features = len(NUMERIC_FEATURES) + len(BOOLEAN_FEATURES) + len(CATEGORICAL_FEATURES)
        X = np.zeros((len(rows), n_features), dtype=np.float64)
        y = np.zeros(len(rows), dtype=np.float64)
        weights = np.ones(len(rows), dtype=np.float64)
        for i, row in enumerate(rows):
            X[i, :] = [float(v) if v is not None else np.nan for v in row[:n_features]]
            y[i] = row[n_features]
            weights[i] = row[n_features + 1]
        return X, y, weights

    X_train, y_train, train_weights = fetch_split(train_dates, max_train_rows)
    X_val, y_val, val_weights = fetch_split(val_dates, max_val_rows)

    print(f"Training set: {len(y_train)} rows, Validation set: {len(y_val)} rows")
    if late_delay_weight != 1.0:
        print(
            "Late-delay weighting: "
            f"{late_delay_weight:g}x for target delay > {late_delay_threshold_seconds:g}s"
        )

    # Create LightGBM datasets
    feature_names = NUMERIC_FEATURES + BOOLEAN_FEATURES + CATEGORICAL_FEATURES
    cat_indices = [feature_names.index(f) for f in CATEGORICAL_FEATURES]

    train_data = lgb.Dataset(
        X_train, label=y_train,
        weight=train_weights,
        feature_name=feature_names,
        categorical_feature=[feature_names[i] for i in cat_indices],
        free_raw_data=False,
    )
    val_data = lgb.Dataset(
        X_val, label=y_val,
        weight=val_weights,
        feature_name=feature_names,
        categorical_feature=[feature_names[i] for i in cat_indices],
        reference=train_data,
        free_raw_data=False,
    )

    params = {
        "objective": "mae",
        "metric": "mae",
        "num_leaves": 31,
        "min_data_in_leaf": 50,
        "learning_rate": 0.05,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "verbose": -1,
    }

    callbacks = [lgb.log_evaluation(50)]
    if train_dates != val_dates:
        callbacks.append(lgb.early_stopping(20))

    model = lgb.train(
        params,
        train_data,
        num_boost_round=200,
        valid_sets=[val_data],
        valid_names=["val"],
        callbacks=callbacks,
    )

    # Save model
    model_path = output_root / "lgbm_model.txt"
    model.save_model(str(model_path))
    print(f"Saved model to {model_path}")

    # Quantile models for prediction intervals, trained on the same datasets.
    quantile_results = {}
    quantile_val_preds = {}
    for q in quantiles:
        print(f"\nTraining quantile model (alpha={q})...")
        q_params = dict(params, objective="quantile", alpha=q, metric="quantile")
        q_callbacks = [lgb.log_evaluation(50)]
        if train_dates != val_dates:
            q_callbacks.append(lgb.early_stopping(20))
        q_model = lgb.train(
            q_params,
            train_data,
            num_boost_round=200,
            valid_sets=[val_data],
            valid_names=["val"],
            callbacks=q_callbacks,
        )
        q_path = output_root / quantile_model_filename(q)
        q_model.save_model(str(q_path))
        print(f"Saved quantile model to {q_path}")

        q_pred = q_model.predict(X_val)
        quantile_val_preds[q] = q_pred
        residual = y_val - q_pred
        pinball = np.mean(np.maximum(q * residual, (q - 1) * residual))
        quantile_results[q] = {
            "pinball_loss": float(pinball),
            "coverage_below": float(np.mean(y_val <= q_pred)),
            "best_iteration": q_model.best_iteration,
        }

    interval_results = None
    if len(quantiles) >= 2:
        lo_q, hi_q = min(quantiles), max(quantiles)
        lo_pred = np.minimum(quantile_val_preds[lo_q], quantile_val_preds[hi_q])
        hi_pred = np.maximum(quantile_val_preds[lo_q], quantile_val_preds[hi_q])
        interval_results = {
            "lower_quantile": lo_q,
            "upper_quantile": hi_q,
            "nominal_coverage": hi_q - lo_q,
            "empirical_coverage": float(np.mean((y_val >= lo_pred) & (y_val <= hi_pred))),
            "mean_width_seconds": float(np.mean(hi_pred - lo_pred)),
            "median_width_seconds": float(np.median(hi_pred - lo_pred)),
        }

    # Evaluate
    val_pred = model.predict(X_val)
    val_mae = np.mean(np.abs(y_val - val_pred))
    val_weighted_mae = np.average(np.abs(y_val - val_pred), weights=val_weights)
    val_rmse = np.sqrt(np.mean((y_val - val_pred) ** 2))
    val_miss_risk_mean = np.mean(np.maximum(val_pred - y_val, 0))
    val_wait_risk_mean = np.mean(np.maximum(y_val - val_pred, 0))

    train_pred = model.predict(X_train)
    train_mae = np.mean(np.abs(y_train - train_pred))
    train_weighted_mae = np.average(np.abs(y_train - train_pred), weights=train_weights)

    # Feature importance
    importance = dict(zip(feature_names, model.feature_importance(importance_type="gain")))
    importance = dict(sorted(importance.items(), key=lambda x: -x[1]))

    return {
        "train_mae": float(train_mae),
        "train_weighted_mae": float(train_weighted_mae),
        "val_mae": float(val_mae),
        "val_weighted_mae": float(val_weighted_mae),
        "val_rmse": float(val_rmse),
        "val_miss_risk_mean": float(val_miss_risk_mean),
        "val_wait_risk_mean": float(val_wait_risk_mean),
        "late_delay_threshold_seconds": float(late_delay_threshold_seconds),
        "late_delay_weight": float(late_delay_weight),
        "quantiles": {str(q): metrics for q, metrics in quantile_results.items()},
        "interval": interval_results,
        "feature_importance": importance,
        "train_rows": len(y_train),
        "val_rows": len(y_val),
        "train_dates": train_dates,
        "val_dates": val_dates,
        "best_iteration": model.best_iteration,
    }


def write_evaluation(output_root, baseline_results, lgbm_results, train_dates, val_dates):
    """Write evaluation comparison report."""
    lines = ["Delay Prediction Model Evaluation", "=" * 40, ""]
    lines.append(f"Train dates: {train_dates[0]}..{train_dates[-1]} ({len(train_dates)} days)")
    lines.append(f"Val dates:   {val_dates[0]}..{val_dates[-1]} ({len(val_dates)} days)")
    lines.append("")

    lines.append("Validation MAE (seconds) — lower is better:")
    lines.append("-" * 40)

    all_results = {**baseline_results}
    if lgbm_results:
        all_results["LightGBM"] = lgbm_results["val_mae"]

    for name, mae in sorted(all_results.items(), key=lambda x: x[1]):
        lines.append(f"  {name:30s} {mae:8.1f}s")

    if lgbm_results:
        lines.append("")
        lines.append(f"LightGBM Details:")
        lines.append(f"  Train MAE: {lgbm_results['train_mae']:.1f}s")
        lines.append(f"  Train weighted MAE: {lgbm_results['train_weighted_mae']:.1f}s")
        lines.append(f"  Val MAE:   {lgbm_results['val_mae']:.1f}s")
        lines.append(f"  Val weighted MAE:   {lgbm_results['val_weighted_mae']:.1f}s")
        lines.append(f"  Val RMSE:  {lgbm_results['val_rmse']:.1f}s")
        lines.append(
            "  Miss-risk mean overprediction: "
            f"{lgbm_results['val_miss_risk_mean']:.1f}s"
        )
        lines.append(
            "  Wait-risk mean underprediction: "
            f"{lgbm_results['val_wait_risk_mean']:.1f}s"
        )
        lines.append(
            "  Late-delay weighting: "
            f"{lgbm_results['late_delay_weight']:.1f}x for target delay "
            f"> {lgbm_results['late_delay_threshold_seconds']:.0f}s"
        )
        lines.append(f"  Train rows: {lgbm_results['train_rows']}")
        lines.append(f"  Val rows:   {lgbm_results['val_rows']}")
        lines.append(f"  Best iteration: {lgbm_results['best_iteration']}")
        if lgbm_results["quantiles"]:
            lines.append("")
            lines.append("Quantile Models (validation):")
            for q, metrics in lgbm_results["quantiles"].items():
                lines.append(
                    f"  q={q}: pinball loss {metrics['pinball_loss']:.1f}s, "
                    f"fraction of actuals at/below prediction {metrics['coverage_below']:.3f} "
                    f"(target {float(q):.2f})"
                )
            interval = lgbm_results["interval"]
            if interval:
                lines.append(
                    f"  Interval [q{interval['lower_quantile']:.2f}, q{interval['upper_quantile']:.2f}]: "
                    f"empirical coverage {interval['empirical_coverage']:.3f} "
                    f"(nominal {interval['nominal_coverage']:.2f}), "
                    f"mean width {interval['mean_width_seconds']:.0f}s, "
                    f"median width {interval['median_width_seconds']:.0f}s"
                )
        lines.append("")
        lines.append("Feature Importance (gain):")
        for feat, gain in lgbm_results["feature_importance"].items():
            lines.append(f"  {feat:30s} {gain:12.0f}")

    report = "\n".join(lines) + "\n"
    report_path = output_root / "evaluation.txt"
    report_path.write_text(report)
    print(f"\n{report}")
    print(f"Saved evaluation to {report_path}")

    # Also save as JSON for programmatic access
    json_path = output_root / "evaluation.json"
    json_data = {
        "baselines": {k: float(v) for k, v in baseline_results.items()},
        "train_dates": train_dates,
        "val_dates": val_dates,
    }
    if lgbm_results:
        json_data["lgbm"] = {
            "train_mae": lgbm_results["train_mae"],
            "train_weighted_mae": lgbm_results["train_weighted_mae"],
            "val_mae": lgbm_results["val_mae"],
            "val_weighted_mae": lgbm_results["val_weighted_mae"],
            "val_rmse": lgbm_results["val_rmse"],
            "val_miss_risk_mean": lgbm_results["val_miss_risk_mean"],
            "val_wait_risk_mean": lgbm_results["val_wait_risk_mean"],
            "late_delay_threshold_seconds": lgbm_results["late_delay_threshold_seconds"],
            "late_delay_weight": lgbm_results["late_delay_weight"],
            "quantiles": lgbm_results["quantiles"],
            "interval": lgbm_results["interval"],
            "feature_importance": {k: float(v) for k, v in lgbm_results["feature_importance"].items()},
        }
    json_path.write_text(json.dumps(json_data, indent=2))


def parse_args():
    parser = argparse.ArgumentParser(description="Train delay prediction model.")
    parser.add_argument(
        "--features-root", type=Path, default=DEFAULT_FEATURES_ROOT,
        help="Root directory of feature table Parquet files.",
    )
    parser.add_argument(
        "--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT,
        help="Output directory for model artifacts.",
    )
    parser.add_argument(
        "--skip-lgbm", action="store_true",
        help="Only compute baselines, skip LightGBM training.",
    )
    parser.add_argument(
        "--max-train-rows", type=int,
        help="Deterministically sample at most this many training rows for LightGBM.",
    )
    parser.add_argument(
        "--max-val-rows", type=int,
        help="Deterministically sample at most this many validation rows for LightGBM.",
    )
    parser.add_argument(
        "--late-delay-threshold-seconds", type=float, default=300.0,
        help="Rows with target delay above this threshold receive late-delay weighting.",
    )
    parser.add_argument(
        "--late-delay-weight", type=float, default=1.0,
        help="Training/evaluation weight for rows above --late-delay-threshold-seconds.",
    )
    parser.add_argument(
        "--quantiles", default="0.1,0.9",
        help="Comma-separated quantiles for prediction-interval models. "
             "Empty string skips quantile training.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    features_root = args.features_root.resolve()
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.sql("SET timezone = 'UTC'")

    count, num_dates = load_features(con, features_root)
    if count == 0:
        print("No feature data found.")
        raise SystemExit(1)

    train_dates, val_dates = get_time_split(con)

    print(f"Train dates: {train_dates[0]}..{train_dates[-1]} ({len(train_dates)} days)")
    print(f"Val dates:   {val_dates[0]}..{val_dates[-1]} ({len(val_dates)} days)")

    print("\nBuilding baselines...")
    build_baselines(con, output_root, train_dates)
    baseline_results = evaluate_baselines(con, val_dates)

    print("\nBaseline validation MAE (seconds):")
    for name, mae in sorted(baseline_results.items(), key=lambda x: x[1]):
        print(f"  {name}: {mae:.1f}s")

    quantiles = sorted(float(q) for q in args.quantiles.split(",") if q.strip())
    if any(not 0 < q < 1 for q in quantiles):
        raise SystemExit(f"Quantiles must be in (0, 1), got: {quantiles}")

    lgbm_results = None
    if not args.skip_lgbm:
        print("\nTraining LightGBM model...")
        lgbm_results = train_lgbm(
            con,
            output_root,
            train_dates,
            val_dates,
            args.max_train_rows,
            args.max_val_rows,
            args.late_delay_threshold_seconds,
            args.late_delay_weight,
            quantiles,
        )

    write_evaluation(output_root, baseline_results, lgbm_results, train_dates, val_dates)


if __name__ == "__main__":
    main()
