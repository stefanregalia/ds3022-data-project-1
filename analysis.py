#!/usr/bin/env python3
"""
analysis.py — Final analysis for NYC Taxi CO₂ (2015–2024)

- Uses ENRICHED fact table (default: fct_trips_enriched)
- No materialized TEMP table (avoids massive spill files)
- "Largest trip" computed per taxi_type via streaming Top-1 (no huge sort)
- Clear logging + timings, plot saved to plots/monthly_co2_by_type.png
"""

import os
import time
import logging
from pathlib import Path

import duckdb
import pandas as pd
import matplotlib.pyplot as plt

# -----------------------------
# Config
# -----------------------------
DB_PATH = "emissions.duckdb"
if not os.path.exists(DB_PATH):
    raise FileNotFoundError(f"Expected DuckDB at {DB_PATH}")

FACT_TABLE = os.getenv("FACT_TABLE", "fct_trips_enriched")

PLOT_DIR = Path("plots"); PLOT_DIR.mkdir(parents=True, exist_ok=True)
PLOT_FILE = PLOT_DIR / "monthly_co2_by_type.png"

START_TS = "2015-01-01"
END_TS   = "2025-01-01"

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="analysis.log"
)
logger = logging.getLogger(__name__)

def sizeof_fmt(num: int, suffix="B") -> str:
    for unit in ["","K","M","G","T","P","E","Z"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f} Y{suffix}"

def _qualify_table(con, table_name: str) -> str:
    q = con.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
        ORDER BY (table_schema = 'main') DESC
        LIMIT 1
    """, [table_name]).df()
    if q.empty:
        raise RuntimeError(f"Could not find table named '{table_name}' in DuckDB.")
    return f"{q.loc[0,'table_schema']}.\"{q.loc[0,'table_name']}\""

def _dow_label(x):
    labels = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"]
    try:
        xi = int(x)
        if 0 <= xi <= 6:
            return labels[xi]
        if 1 <= xi <= 7:
            return labels[xi % 7]
    except Exception:
        pass
    return str(x)

def _fmt_int(x):
    return "NA" if pd.isna(x) else str(int(x))

def print_and_log(msg: str):
    print(msg); logger.info(msg)

def run_query(con, label: str, sql: str, params=None) -> pd.DataFrame:
    print_and_log(f"START: {label}")
    t0 = time.perf_counter()
    df = con.execute(sql, params or []).df()
    dt = time.perf_counter() - t0
    print_and_log(f"END:   {label} | {dt:0.2f}s | rows={len(df)}")
    return df

def main():
    # Connect
    db_size = sizeof_fmt(os.path.getsize(DB_PATH))
    print_and_log(f"Connecting to DuckDB at: {DB_PATH} (size: {db_size})")
    con = duckdb.connect(DB_PATH, read_only=True)

    # Performance PRAGMAs
    try:
        con.execute(f"PRAGMA threads={os.cpu_count()}"); print_and_log(f"PRAGMA threads set to {os.cpu_count()}")
    except Exception as e:
        print_and_log(f"PRAGMA threads not set: {e}")
    try:
        con.execute("PRAGMA memory_limit='6GB'"); print_and_log("PRAGMA memory_limit set to 6GB")
    except Exception as e:
        print_and_log(f"PRAGMA memory_limit not set: {e}")
    try:
        os.makedirs("/tmp/duckdb_tmp", exist_ok=True)
        con.execute("PRAGMA temp_directory='/tmp/duckdb_tmp'")
        print_and_log("PRAGMA temp_directory set to /tmp/duckdb_tmp")
    except Exception as e:
        print_and_log(f"PRAGMA temp_directory not set: {e}")

    # Fact table
    table = _qualify_table(con, FACT_TABLE)
    print_and_log(f"Using table (enriched): {table}")

    # Reusable core CTE (not materialized)
    core_cte = f"""
        WITH core AS (
          SELECT
            taxi_type,
            pickup_ts,
            trip_distance,
            avg_mph,
            trip_co2_kgs,
            hour_of_day,
            day_of_week,
            week_of_year,
            month_of_year
          FROM {table}
          WHERE pickup_ts >= TIMESTAMP '{START_TS}'
            AND pickup_ts <  TIMESTAMP '{END_TS}'
        )
    """

    # 1) Largest carbon-producing trip (per taxi_type) — per-type Top-1
    largest_rows = []
    for ttype in ("YELLOW", "GREEN"):
        largest_one_sql = core_cte + """
            SELECT
                taxi_type,
                pickup_ts,
                trip_distance,
                avg_mph,
                trip_co2_kgs
            FROM core
            WHERE taxi_type = ?
            ORDER BY trip_co2_kgs DESC, pickup_ts ASC
            LIMIT 1;
        """
        df_one = run_query(con, f"Largest carbon-producing trip — {ttype}", largest_one_sql, [ttype])
        largest_rows.append(df_one)
    largest = pd.concat(largest_rows, ignore_index=True)

    # 2) Hour-of-day heavy/light (avg CO2 per trip)
    hour_sql = core_cte + """
        , stats AS (
            SELECT taxi_type, hour_of_day AS hour, AVG(trip_co2_kgs) AS avg_co2
            FROM core
            GROUP BY 1,2
        ),
        ranked AS (
            SELECT *,
                   RANK() OVER (PARTITION BY taxi_type ORDER BY avg_co2 DESC) AS r_desc,
                   RANK() OVER (PARTITION BY taxi_type ORDER BY avg_co2 ASC)  AS r_asc
            FROM stats
        )
        SELECT taxi_type,
               MAX(CASE WHEN r_desc=1 THEN hour END)     AS heavy_hour,
               MAX(CASE WHEN r_desc=1 THEN avg_co2 END)  AS heavy_hour_avg_co2,
               MAX(CASE WHEN r_asc =1 THEN hour END)     AS light_hour,
               MAX(CASE WHEN r_asc =1 THEN avg_co2 END)  AS light_hour_avg_co2
        FROM ranked
        GROUP BY taxi_type
        ORDER BY taxi_type;
    """
    hour_stats = run_query(con, "Hour-of-day heavy/light (avg CO2 per trip)", hour_sql)

    # 3) Day-of-week heavy/light (avg CO2 per trip)
    dow_sql = core_cte + """
        , stats AS (
            SELECT taxi_type, day_of_week AS dow, AVG(trip_co2_kgs) AS avg_co2
            FROM core
            GROUP BY 1,2
        ),
        ranked AS (
            SELECT *,
                   RANK() OVER (PARTITION BY taxi_type ORDER BY avg_co2 DESC) AS r_desc,
                   RANK() OVER (PARTITION BY taxi_type ORDER BY avg_co2 ASC)  AS r_asc
            FROM stats
        )
        SELECT taxi_type,
               MAX(CASE WHEN r_desc=1 THEN dow END)      AS heavy_dow,
               MAX(CASE WHEN r_desc=1 THEN avg_co2 END)  AS heavy_dow_avg_co2,
               MAX(CASE WHEN r_asc =1 THEN dow END)      AS light_dow,
               MAX(CASE WHEN r_asc =1 THEN avg_co2 END)  AS light_dow_avg_co2
        FROM ranked
        GROUP BY taxi_type
        ORDER BY taxi_type;
    """
    dow_stats = run_query(con, "Day-of-week heavy/light (avg CO2 per trip)", dow_sql)

    # 4) Week-of-year heavy/light (avg CO2 per trip)
    woy_sql = core_cte + """
        , stats AS (
            SELECT taxi_type, week_of_year AS woy, AVG(trip_co2_kgs) AS avg_co2
            FROM core
            GROUP BY 1,2
        ),
        ranked AS (
            SELECT *,
                   RANK() OVER (PARTITION BY taxi_type ORDER BY avg_co2 DESC) AS r_desc,
                   RANK() OVER (PARTITION BY taxi_type ORDER BY avg_co2 ASC)  AS r_asc
            FROM stats
        )
        SELECT taxi_type,
               MAX(CASE WHEN r_desc=1 THEN woy END)      AS heavy_woy,
               MAX(CASE WHEN r_desc=1 THEN avg_co2 END)  AS heavy_woy_avg_co2,
               MAX(CASE WHEN r_asc =1 THEN woy END)      AS light_woy,
               MAX(CASE WHEN r_asc =1 THEN avg_co2 END)  AS light_woy_avg_co2
        FROM ranked
        GROUP BY taxi_type
        ORDER BY taxi_type;
    """
    woy_stats = run_query(con, "Week-of-year heavy/light (avg CO2 per trip)", woy_sql)

    # 5) Month-of-year heavy/light (avg CO2 per trip)
    moy_sql = core_cte + """
        , stats AS (
            SELECT taxi_type, month_of_year AS moy, AVG(trip_co2_kgs) AS avg_co2
            FROM core
            GROUP BY 1,2
        ),
        ranked AS (
            SELECT *,
                   RANK() OVER (PARTITION BY taxi_type ORDER BY avg_co2 DESC) AS r_desc,
                   RANK() OVER (PARTITION BY taxi_type ORDER BY avg_co2 ASC)  AS r_asc
            FROM stats
        )
        SELECT taxi_type,
               MAX(CASE WHEN r_desc=1 THEN moy END)      AS heavy_moy,
               MAX(CASE WHEN r_desc=1 THEN avg_co2 END)  AS heavy_moy_avg_co2,
               MAX(CASE WHEN r_asc =1 THEN moy END)      AS light_moy,
               MAX(CASE WHEN r_asc =1 THEN avg_co2 END)  AS light_moy_avg_co2
        FROM ranked
        GROUP BY taxi_type
        ORDER BY taxi_type;
    """
    moy_stats = run_query(con, "Month-of-year heavy/light (avg CO2 per trip)", moy_sql)

    # Monthly CO2 totals (for plot)
    monthly_totals_sql = core_cte + """
        SELECT
            taxi_type,
            DATE_TRUNC('month', pickup_ts) AS month,
            SUM(trip_co2_kgs) AS total_co2_kg
        FROM core
        GROUP BY 1,2
        ORDER BY month, taxi_type;
    """
    monthly = run_query(con, "Monthly CO2 totals by taxi type (for plotting)", monthly_totals_sql)

    # -------------------------
    # Output (labeled)
    # -------------------------
    print_and_log("===== LARGEST CARBON-PRODUCING TRIP BY TAXI TYPE (2015–2024) =====")
    for _, r in largest.iterrows():
        print_and_log(
            f"{r['taxi_type']}: {r['trip_co2_kgs']:.3f} kg CO2 | "
            f"pickup={r['pickup_ts']} | dist_mi={r['trip_distance']:.2f} | avg_mph={r['avg_mph']:.2f}"
        )

    print_and_log("\n===== HOUR OF DAY (avg CO2 per trip) =====")
    for _, r in hour_stats.iterrows():
        print_and_log(
            f"{r['taxi_type']}: HEAVY hour={_fmt_int(r['heavy_hour'])} "
            f"({r['heavy_hour_avg_co2']:.4f} kg) | "
            f"LIGHT hour={_fmt_int(r['light_hour'])} "
            f"({r['light_hour_avg_co2']:.4f} kg)"
        )

    print_and_log("\n===== DAY OF WEEK (avg CO2 per trip) =====")
    for _, r in dow_stats.iterrows():
        heavy = _dow_label(r['heavy_dow'])
        light = _dow_label(r['light_dow'])
        print_and_log(
            f"{r['taxi_type']}: HEAVY day={heavy} "
            f"({r['heavy_dow_avg_co2']:.4f} kg) | "
            f"LIGHT day={light} "
            f"({r['light_dow_avg_co2']:.4f} kg)"
        )

    print_and_log("\n===== WEEK OF YEAR (avg CO2 per trip) =====")
    for _, r in woy_stats.iterrows():
        print_and_log(
            f"{r['taxi_type']}: HEAVY week={_fmt_int(r['heavy_woy'])} "
            f"({r['heavy_woy_avg_co2']:.4f} kg) | "
            f"LIGHT week={_fmt_int(r['light_woy'])} "
            f"({r['light_woy_avg_co2']:.4f} kg)"
        )

    print_and_log("\n===== MONTH OF YEAR (avg CO2 per trip) =====")
    for _, r in moy_stats.iterrows():
        print_and_log(
            f"{r['taxi_type']}: HEAVY month={_fmt_int(r['heavy_moy'])} "
            f"({r['heavy_moy_avg_co2']:.4f} kg) | "
            f"LIGHT month={_fmt_int(r['light_moy'])} "
            f"({r['light_moy_avg_co2']:.4f} kg)"
        )

    # -------------------------
    # Plot
    # -------------------------
    if monthly.empty:
        print_and_log("No monthly data found to plot. Exiting before plot.")
        return

    print_and_log("Preparing monthly totals for plotting...")
    monthly['month'] = pd.to_datetime(monthly['month'])
    wide = monthly.pivot(index='month', columns='taxi_type', values='total_co2_kg').sort_index()
    print_and_log(f"Monthly dataframe shape for plot: {wide.shape}")

    print_and_log(f"Creating plot: {PLOT_FILE}")
    plt.figure(figsize=(12, 5))
    plt.plot(wide.index, wide.get('YELLOW'), label='YELLOW')
    plt.plot(wide.index, wide.get('GREEN'), label='GREEN')
    plt.title("Monthly CO2 Totals by Taxi Type (2015–2024)")
    plt.xlabel("Month")
    plt.ylabel("Total CO2 (kg)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(PLOT_FILE, dpi=150)
    plt.close()
    print_and_log(f"Saved plot to: {PLOT_FILE}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("Analysis failed")
        raise
