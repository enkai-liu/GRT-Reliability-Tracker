"""Train delay prediction model from the enriched feature table.

Evaluates baseline models and a LightGBM model, saves the best performer.

Baselines:
  0. Schedule (predict delay = 0)
  1. Route-hour average (median delay per route_id, hour_of_day, is_weekend)
  2. Route-stop-hour average (median delay per route_id, stop_id, hour_of_day, is_weekend)

Primary model:
  LightGBM gradient boosted trees with MAE objective.

Output:
  - data/analysis/models/lgbm_model.txt (LightGBM model)
  - data/analysis/models/route_hour_avg.parquet (baseline lookup)
  - data/analysis/models/route_stop_hour_avg.parquet (baseline lookup)
  - data/analysis/models/evaluation.txt (comparison report)
"""

import argparse
import json
from pathlib import Path

import duckdb
import lightgbm as lgb
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FEATURES_ROOT = PROJECT_ROOT / "data" / "analysis" / "features"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data" / "analysis" / "models"

# Features for LightGBM
NUMERIC_FEATURES = [
    "stop_sequence_normalized",
    "hour_of_day",
    "minute_of_hour",
    "day_of_week",
    "temperature_c",
    "wind_speed_kmh",
    "precip_probability_pct",
    "prediction_lead_minutes",
]

BOOLEAN_FEATURES = [
    "is_weekend",
    "is_rush_hour",
    "is_rain",
    "is_snow",
    "is_precip",
]

CATEGORICAL_FEATURES = [
    "route_id",
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


def build_baselines(con, output_root):
    """Compute and save baseline lookup tables."""

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
        GROUP BY route_id, stop_id, hour_of_day, is_weekend
    """)
    con.sql(f"COPY route_stop_hour_avg TO '{output_root}/route_stop_hour_avg.parquet' (FORMAT PARQUET)")
    rsha_count = con.sql("SELECT count(*) FROM route_stop_hour_avg").fetchone()[0]
    print(f"Baseline 2 (route-stop-hour avg): {rsha_count} lookup entries")


def evaluate_baselines(con):
    """Compute MAE for each baseline on the full dataset."""
    results = {}

    # Baseline 0: schedule (predict 0)
    mae_0 = con.sql("SELECT avg(abs(delay_seconds)) FROM features").fetchone()[0]
    results["schedule (delay=0)"] = mae_0

    # Baseline 1: route-hour average
    mae_1 = con.sql("""
        SELECT avg(abs(f.delay_seconds - COALESCE(b.median_delay, 0)))
        FROM features f
        LEFT JOIN route_hour_avg b
            ON f.route_id = b.route_id
            AND f.hour_of_day = b.hour_of_day
            AND f.is_weekend = b.is_weekend
    """).fetchone()[0]
    results["route-hour avg"] = mae_1

    # Baseline 2: route-stop-hour average
    mae_2 = con.sql("""
        SELECT avg(abs(f.delay_seconds - COALESCE(b.median_delay, 0)))
        FROM features f
        LEFT JOIN route_stop_hour_avg b
            ON f.route_id = b.route_id
            AND f.stop_id = b.stop_id
            AND f.hour_of_day = b.hour_of_day
            AND f.is_weekend = b.is_weekend
    """).fetchone()[0]
    results["route-stop-hour avg"] = mae_2

    return results


def prepare_lgbm_data(con):
    """Extract feature matrix and target for LightGBM."""
    feature_cols = ", ".join(ALL_FEATURES)

    # Convert booleans to int and categoricals to codes in DuckDB
    select_parts = []
    for f in NUMERIC_FEATURES:
        select_parts.append(f"COALESCE({f}, 0) AS {f}")
    for f in BOOLEAN_FEATURES:
        select_parts.append(f"CAST(COALESCE({f}, false) AS INTEGER) AS {f}")
    for f in CATEGORICAL_FEATURES:
        select_parts.append(f)

    select_sql = ", ".join(select_parts)

    result = con.sql(f"""
        SELECT {select_sql}, {TARGET}, snapshot_date
        FROM features
    """).fetchall()

    columns = [f for f in ALL_FEATURES] + [TARGET, "snapshot_date"]
    return result, columns


def train_lgbm(con, output_root, num_dates):
    """Train LightGBM model with time-based split."""
    import numpy as np

    # Get sorted unique dates for time-based split
    dates = [row[0] for row in con.sql(
        "SELECT DISTINCT snapshot_date FROM features ORDER BY snapshot_date"
    ).fetchall()]

    if len(dates) < 2:
        print("WARNING: Only 1 date available. Using full dataset for training (no validation).")
        train_dates = dates
        val_dates = dates  # same data for "validation" — just to get a number
    else:
        # Use last ~20% of dates as validation
        split_idx = max(1, int(len(dates) * 0.8))
        train_dates = dates[:split_idx]
        val_dates = dates[split_idx:]

    print(f"Train dates: {train_dates[0]}..{train_dates[-1]} ({len(train_dates)} days)")
    print(f"Val dates:   {val_dates[0]}..{val_dates[-1]} ({len(val_dates)} days)")

    # Extract arrays
    feature_select = []
    for f in NUMERIC_FEATURES:
        feature_select.append(f"COALESCE({f}, 0)::DOUBLE AS {f}")
    for f in BOOLEAN_FEATURES:
        feature_select.append(f"CAST(COALESCE({f}, false) AS DOUBLE) AS {f}")
    for f in CATEGORICAL_FEATURES:
        # Encode categoricals as integers for LightGBM
        if f == "route_id":
            feature_select.append(f"CAST({f} AS INTEGER) AS {f}")
        elif f == "transit_mode":
            feature_select.append(f"CASE WHEN {f} = 'bus' THEN 0 ELSE 1 END AS {f}")
        else:
            feature_select.append(f"hash({f}) % 10000 AS {f}")

    select_sql = ", ".join(feature_select)

    def fetch_split(date_list):
        dl = ", ".join(f"'{d}'" for d in date_list)
        rows = con.sql(f"""
            SELECT {select_sql}, {TARGET}
            FROM features
            WHERE snapshot_date IN ({dl})
        """).fetchall()

        n_features = len(NUMERIC_FEATURES) + len(BOOLEAN_FEATURES) + len(CATEGORICAL_FEATURES)
        X = np.zeros((len(rows), n_features), dtype=np.float64)
        y = np.zeros(len(rows), dtype=np.float64)
        for i, row in enumerate(rows):
            X[i, :] = [float(v) if v is not None else np.nan for v in row[:n_features]]
            y[i] = row[n_features]
        return X, y

    X_train, y_train = fetch_split(train_dates)
    X_val, y_val = fetch_split(val_dates)

    print(f"Training set: {len(y_train)} rows, Validation set: {len(y_val)} rows")

    # Create LightGBM datasets
    feature_names = NUMERIC_FEATURES + BOOLEAN_FEATURES + CATEGORICAL_FEATURES
    cat_indices = [feature_names.index(f) for f in CATEGORICAL_FEATURES]

    train_data = lgb.Dataset(
        X_train, label=y_train,
        feature_name=feature_names,
        categorical_feature=[feature_names[i] for i in cat_indices],
        free_raw_data=False,
    )
    val_data = lgb.Dataset(
        X_val, label=y_val,
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
    if len(dates) >= 2:
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

    # Evaluate
    import numpy as np
    val_pred = model.predict(X_val)
    val_mae = np.mean(np.abs(y_val - val_pred))
    val_rmse = np.sqrt(np.mean((y_val - val_pred) ** 2))

    train_pred = model.predict(X_train)
    train_mae = np.mean(np.abs(y_train - train_pred))

    # Feature importance
    importance = dict(zip(feature_names, model.feature_importance(importance_type="gain")))
    importance = dict(sorted(importance.items(), key=lambda x: -x[1]))

    return {
        "train_mae": float(train_mae),
        "val_mae": float(val_mae),
        "val_rmse": float(val_rmse),
        "feature_importance": importance,
        "train_rows": len(y_train),
        "val_rows": len(y_val),
        "train_dates": train_dates,
        "val_dates": val_dates,
        "best_iteration": model.best_iteration,
    }


def write_evaluation(output_root, baseline_results, lgbm_results):
    """Write evaluation comparison report."""
    lines = ["Delay Prediction Model Evaluation", "=" * 40, ""]

    lines.append("MAE (seconds) — lower is better:")
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
        lines.append(f"  Val MAE:   {lgbm_results['val_mae']:.1f}s")
        lines.append(f"  Val RMSE:  {lgbm_results['val_rmse']:.1f}s")
        lines.append(f"  Train rows: {lgbm_results['train_rows']}")
        lines.append(f"  Val rows:   {lgbm_results['val_rows']}")
        lines.append(f"  Best iteration: {lgbm_results['best_iteration']}")
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
    }
    if lgbm_results:
        json_data["lgbm"] = {
            "train_mae": lgbm_results["train_mae"],
            "val_mae": lgbm_results["val_mae"],
            "val_rmse": lgbm_results["val_rmse"],
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

    print("\nBuilding baselines...")
    build_baselines(con, output_root)
    baseline_results = evaluate_baselines(con)

    print("\nBaseline MAE (seconds):")
    for name, mae in sorted(baseline_results.items(), key=lambda x: x[1]):
        print(f"  {name}: {mae:.1f}s")

    lgbm_results = None
    if not args.skip_lgbm:
        print("\nTraining LightGBM model...")
        lgbm_results = train_lgbm(con, output_root, num_dates)

    write_evaluation(output_root, baseline_results, lgbm_results)


if __name__ == "__main__":
    main()
