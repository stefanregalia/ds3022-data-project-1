#!/usr/bin/env python3

# Importing libraries
import time
import duckdb
import logging
import os

 # Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="load.log"
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
DB_PATH = os.environ.get("DB_PATH", "emissions.duckdb") # default DB path
SLEEP_SECONDS = float(os.environ.get("SLEEP_SECONDS", "30.0"))  # configurable delay

# Years/months to load (2015–2024)
YEARS  = list(range(2015, 2025))
MONTHS = [f"{m:02d}" for m in range(1, 13)]

# Per-table resume points (YYYY-MM format) - set to skip earlier months when pausing the data loading because of lost connection
RESUME_FROM_YELLOW = os.environ.get("RESUME_FROM_YELLOW")
RESUME_FROM_GREEN  = os.environ.get("RESUME_FROM_GREEN")

# Official TLC CloudFront Parquet URL patterns
YELLOW_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{yyyy}-{mm}.parquet"
GREEN_URL  = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{yyyy}-{mm}.parquet"

# Pause/Stop flags for interactive control
PAUSE_FILE = "PAUSE.LOAD"
STOP_FILE  = "STOP.LOAD"

def _maybe_pause():
    # Pause between each file (every month)
    while os.path.exists(PAUSE_FILE):
        logger.info("PAUSED: remove PAUSE.LOAD to continue…")
        time.sleep(3)

# Columns to keep (trimmed to reduce DB size and speed up queries)
COMMON_KEEP_COLS = [
    "VendorID",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID", "DOLocationID",
    "payment_type",
    "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]

# Desired column types (broad to allow coercion from files)
TYPE_MAP_BASE = {
    "VendorID": "INTEGER",
    "passenger_count": "INTEGER",
    "trip_distance": "DOUBLE",
    "RatecodeID": "INTEGER",
    "store_and_fwd_flag": "VARCHAR",
    "PULocationID": "INTEGER",
    "DOLocationID": "INTEGER",
    "payment_type": "INTEGER",
    "fare_amount": "DOUBLE",
    "extra": "DOUBLE",
    "mta_tax": "DOUBLE",
    "tip_amount": "DOUBLE",
    "tolls_amount": "DOUBLE",
    "improvement_surcharge": "DOUBLE",
    "total_amount": "DOUBLE",
    "congestion_surcharge": "DOUBLE",
    "airport_fee": "DOUBLE",
}

# Timestamps added per table below
def _keep_cols_for(table_name: str):
    if "yellow" in table_name:
        ts_pick = "tpep_pickup_datetime"
        ts_drop = "tpep_dropoff_datetime"
    else:
        ts_pick = "lpep_pickup_datetime"
        ts_drop = "lpep_dropoff_datetime"
    keep = [ts_pick, ts_drop] + COMMON_KEEP_COLS
    return keep, ts_pick  # also return pickup col

# Maps timestamp columns per table
def _type_map_for(table_name: str):
    tmap = dict(TYPE_MAP_BASE)
    if "yellow" in table_name:
        tmap["tpep_pickup_datetime"]  = "TIMESTAMP"
        tmap["tpep_dropoff_datetime"] = "TIMESTAMP"
    else:
        tmap["lpep_pickup_datetime"]  = "TIMESTAMP"
        tmap["lpep_dropoff_datetime"] = "TIMESTAMP"
    return tmap

# Table schema management, in case a table shows an error on insert
def _ensure_table_schema(con: duckdb.DuckDBPyConnection, table_name: str, keep_cols, type_map):
    """
    Create the table if it doesn't exist with all keep_cols and our desired types.
    If it exists, add any missing keep columns.
    """
    exists = bool(con.execute(f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = '{table_name}'
    """).fetchone()[0])

    if not exists:
        cols_def = ",\n".join(f'"{c}" {type_map[c]}' for c in keep_cols)
        con.execute(f'CREATE TABLE {table_name} ({cols_def});')
        return

    # Adding any missing keep columns (DuckDB PRAGMA table_info: column name at index 1)
    present = {row[1] for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()}
    for c in keep_cols:
        if c not in present:
            con.execute(f'ALTER TABLE {table_name} ADD COLUMN "{c}" {type_map[c]};')

# Build a SELECT list that projects only keep_cols in the right order.
def _build_projection_for_file(con: duckdb.DuckDBPyConnection, url: str, keep_cols, type_map) -> str:
    """
    Build a SELECT list that projects only keep_cols in the right order.
    Use CAST(NULL AS type) AS col for any keep column missing from the file.
    """
    file_desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{url}')").fetchall()
    file_cols = {row[0] for row in file_desc}
    exprs = []
    for c in keep_cols:
        if c in file_cols:
            exprs.append(f'"{c}"')
        else:
            exprs.append(f'CAST(NULL AS {type_map[c]}) AS "{c}"')
    return ", ".join(exprs)

# DuckDB connection setup
def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=DB_PATH, read_only=False)
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("PRAGMA enable_progress_bar=true;")
    con.execute("PRAGMA enable_object_cache=true;")
    con.execute("SET http_keep_alive=true;")

    # If running out of storage, move temp files to a different directory
    tmp_dir = os.environ.get("DUCKDB_TEMP_DIR")
    if tmp_dir:
        os.makedirs(tmp_dir, exist_ok=True)
        con.execute(f"PRAGMA temp_directory='{tmp_dir}';")

    version = con.execute("SELECT version()").fetchone()[0]
    logger.info(f"DuckDB version: {version}")
    print(f"Using DB_PATH={DB_PATH} (DuckDB {version})")
    return con

# Rate-limited, sequential inserter (resume-safe + trimmed columns)

def _insert_rate_limited(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    base_url: str,
    sleep_seconds: float = SLEEP_SECONDS
) -> None:
    """
    2015–2024, month-by-month with a pause between months.
    Resume-safe: deletes that month’s rows before re-inserting.
    Projects only the necessary columns to keep DB small.
    Supports PAUSE.LOAD and STOP.LOAD files for control.
    """
    created = False
    skipped = []

    keep_cols, pickup_col = _keep_cols_for(table_name)
    type_map = _type_map_for(table_name)

    for yyyy in YEARS:
        for mm in MONTHS:
            # honor STOP after previous month
            if os.path.exists(STOP_FILE):
                logger.info("STOP.LOAD detected — exiting gracefully.")
                print("STOP requested — exiting after current checkpoint.")
                return

            _maybe_pause()  # optional pause between months

            # Per-table resume gate
            resume_from = RESUME_FROM_YELLOW if "yellow" in table_name else RESUME_FROM_GREEN
            if resume_from:
                ry, rm = map(int, resume_from.split("-"))
                if (yyyy < ry) or (yyyy == ry and int(mm) < rm):
                    time.sleep(0.01)  # tiny yield so logs stay readable
                    continue

            url = base_url.format(yyyy=yyyy, mm=mm)
            month_start = f"{yyyy}-{mm}-01"

            try:
                # Ensure table exists with target schema
                if not created:
                    _ensure_table_schema(con, table_name, keep_cols, type_map)
                    created = True

                # Build projection (NULL casts for missing columns in this file)
                select_list = _build_projection_for_file(con, url, keep_cols, type_map)

                # Idempotency: remove any previously inserted rows for this month
                con.execute(f"""
                    DELETE FROM {table_name}
                    WHERE "{pickup_col}" >= DATE '{month_start}'
                      AND "{pickup_col}" <  (DATE '{month_start}' + INTERVAL 1 MONTH);
                """)

                logger.info(f"Inserting {table_name} ← {url}")
                con.execute(f"""
                    INSERT INTO {table_name}
                    SELECT {select_list}
                    FROM read_parquet('{url}');
                """)

                # Keep WAL small after each month
                con.execute("PRAGMA force_checkpoint;")

                logger.info(f"OK: {table_name} {yyyy}-{mm}")
                print(f"OK: {table_name} {yyyy}-{mm}")

            except Exception as e:
                logger.warning(f"SKIP {table_name} {yyyy}-{mm}: {e}")
                print(f"SKIP: {table_name} {yyyy}-{mm} ({str(e)[:160]})")
                skipped.append((yyyy, mm))

            time.sleep(sleep_seconds)  # gentle rate limit

    if not created:
        raise RuntimeError(f"Could not create {table_name}; no reachable months at all.")
    if skipped:
        logger.warning(f"{table_name}: skipped months: {skipped}")
        print(f"{table_name}: skipped {len(skipped)} months (first few: {skipped[:6]})")

# Yellow / Green loaders

def load_yellow(con: duckdb.DuckDBPyConnection) -> None:
    _insert_rate_limited(con, "raw_yellow_all", YELLOW_URL, sleep_seconds=SLEEP_SECONDS)

def load_green(con: duckdb.DuckDBPyConnection) -> None:
    _insert_rate_limited(con, "raw_green_all", GREEN_URL, sleep_seconds=SLEEP_SECONDS)

# Vehicle emissions loader (CSV -> normalized 2-column lookup)

def load_vehicle_emissions(con: duckdb.DuckDBPyConnection) -> None:
    """
    Load data/vehicle_emissions.csv into a simple lookup table
    with two rows: yellow and green taxis and their average CO2 grams/mile.
    """
    logger.info("Loading vehicle_emissions from data/vehicle_emissions.csv")
    con.execute("DROP TABLE IF EXISTS vehicle_emissions;")
    con.execute("""
        CREATE TABLE vehicle_emissions AS
        WITH src AS (
            SELECT
                CASE
                    WHEN lower(vehicle_type) LIKE '%yellow%' THEN 'yellow'
                    WHEN lower(vehicle_type) LIKE '%green%'  THEN 'green'
                    ELSE NULL
                END AS taxi_type,
                TRY_CAST(co2_grams_per_mile AS DOUBLE) AS co2_grams_per_mile
            FROM read_csv_auto('data/vehicle_emissions.csv', header=true)
        )
        SELECT
            taxi_type,
            AVG(co2_grams_per_mile) AS co2_grams_per_mile
        FROM src
        WHERE taxi_type IS NOT NULL
          AND co2_grams_per_mile IS NOT NULL
        GROUP BY taxi_type
        ORDER BY taxi_type;
    """)
    logger.info("vehicle_emissions loaded")

# Basic summarization (raw row counts)

def summarize(con: duckdb.DuckDBPyConnection) -> None:
    def _count(tbl: str) -> int:
        try:
            return con.execute(f"SELECT COUNT(*) FROM {tbl};").fetchone()[0]
        except Exception:
            return 0

    y = _count("raw_yellow_all")
    g = _count("raw_green_all")
    e = _count("vehicle_emissions")

    logger.info(f"raw_yellow_all rows: {y:,}")
    logger.info(f"raw_green_all rows:  {g:,}")
    logger.info(f"vehicle_emissions rows: {e:,}")

    print(f"raw_yellow_all rows: {y:,}")
    print(f"raw_green_all rows:  {g:,}")
    print(f"vehicle_emissions rows: {e:,}")

# Main

def main() -> None:
    con = None
    try:
        con = _connect()
        logger.info("Connected to DuckDB instance")

        # 1) Yellow trips 2015–2024 (resume-safe, trimmed columns)
        load_yellow(con)

        # 2) Green trips 2015–2024 (resume-safe, trimmed columns)
        load_green(con)

        # 3) Emissions lookup
        load_vehicle_emissions(con)

        # 4) Quick summary
        summarize(con)

        logger.info("Load stage completed.")
    finally:
        if con is not None:
            con.close()
            logger.info("Closed DuckDB connection.")

if __name__ == "__main__":
    main()
