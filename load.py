#!/usr/bin/env python3
"""
load.py
-------
Load NYC TLC trip data (Yellow & Green, 2015–2024) into DuckDB, plus a small
vehicle emissions lookup table from CSV. Keep it simple and programmatic.

Tables created (max 3):
  - raw_yellow_all
  - raw_green_all
  - vehicle_emissions

Run:
  $ python load.py
"""

import duckdb
import logging
from typing import List

# ------------------------------------------------------------------------------
# Logging (keep it simple: file + INFO; console also prints via basicConfig stdout)
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="load.log"
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
DB_PATH = "emissions.duckdb"

# Years/months to load (2015–2024 inclusive)
YEARS  = list(range(2015, 2025))
MONTHS = [f"{m:02d}" for m in range(1, 13)]

# Official TLC CloudFront Parquet URL patterns (correct ones)
YELLOW_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{yyyy}-{mm}.parquet"
GREEN_URL  = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{yyyy}-{mm}.parquet"


# ------------------------------------------------------------------------------
# Connection helper
# ------------------------------------------------------------------------------
def _connect() -> duckdb.DuckDBPyConnection:
    """
    Connect to our local DuckDB file and turn on httpfs so we can read HTTPS parquet.
    Also enable DuckDB's progress bar so we see long query progress in the console.
    """
    con = duckdb.connect(database=DB_PATH, read_only=False)
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("PRAGMA enable_progress_bar=true;")
    # Optional: object cache can help reuse file metadata across reads
    con.execute("PRAGMA enable_object_cache=true;")

    version = con.execute("SELECT version()").fetchone()[0]
    logger.info(f"DuckDB version: {version}")
    return con


# ------------------------------------------------------------------------------
# Create empty tables (schema from a sample file)
# ------------------------------------------------------------------------------
def _create_empty_from_sample(con: duckdb.DuckDBPyConnection, table_name: str, sample_url: str) -> None:
    """
    Create an empty table using the schema from a known-good sample parquet file.
    We'll use 2015-01 for both Yellow and Green.
    """
    logger.info(f"Creating empty {table_name} from sample: {sample_url}")
    con.execute(f"DROP TABLE IF EXISTS {table_name};")
    con.execute(f"""
        CREATE TABLE {table_name} AS
        SELECT *
        FROM read_parquet('{sample_url}', union_by_name=true)
        WHERE 1=0;
    """)


# ------------------------------------------------------------------------------
# Faster + robust loading: probe URLs first, then insert in batches
# ------------------------------------------------------------------------------
def _collect_available_urls(con: duckdb.DuckDBPyConnection, base_url: str) -> List[str]:
    """
    Probe every (year, month) for 2015–2024 and keep only URLs that are reachable.
    We read LIMIT 1 from each URL; missing or forbidden files are skipped up front.
    """
    urls = []
    total = len(YEARS) * len(MONTHS)
    logger.info(f"Probing up to {total} parquet files for availability...")
    for yyyy in YEARS:
        for mm in MONTHS:
            url = base_url.format(yyyy=yyyy, mm=mm)
            try:
                con.execute(f"SELECT 1 FROM read_parquet('{url}') LIMIT 1;")
                urls.append(url)
            except Exception as e:
                logger.warning(f"Skipping missing/forbidden file: {url} ({e})")
    logger.info(f"Found {len(urls)} available files out of {total}")
    return urls


def _insert_in_batches(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    base_url: str,
    batch_size: int = 24
) -> None:
    """
    Insert all monthly parquet files (2015–2024) into the given table,
    but do it in batches so:
      - we still get parallel reads inside each batch (fast),
      - a single bad file doesn't kill the entire load (robust).
    """
    urls = _collect_available_urls(con, base_url)
    if not urls:
        raise RuntimeError(f"No available files found to load into {table_name}")

    logger.info(f"Inserting {len(urls)} files into {table_name} in batches of {batch_size}")
    for i in range(0, len(urls), batch_size):
        batch = urls[i:i+batch_size]
        logger.info(f"Batch {i // batch_size + 1}: inserting {len(batch)} files")
        con.execute(f"""
            INSERT INTO {table_name}
            SELECT * FROM read_parquet({batch}, union_by_name=true);
        """)


# ------------------------------------------------------------------------------
# Yellow / Green loaders
# ------------------------------------------------------------------------------
def load_yellow(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create empty Yellow table from a sample file, then insert all months in batches.
    """
    _create_empty_from_sample(
        con,
        table_name="raw_yellow_all",
        sample_url=YELLOW_URL.format(yyyy=2015, mm="01"),
    )
    _insert_in_batches(con, "raw_yellow_all", YELLOW_URL, batch_size=24)  # ~2 years per batch


def load_green(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create empty Green table from a sample file, then insert all months in batches.
    """
    _create_empty_from_sample(
        con,
        table_name="raw_green_all",
        sample_url=GREEN_URL.format(yyyy=2015, mm="01"),
    )
    _insert_in_batches(con, "raw_green_all", GREEN_URL, batch_size=24)


# ------------------------------------------------------------------------------
# Vehicle emissions loader (CSV -> normalized 2-column lookup)
# ------------------------------------------------------------------------------
def load_vehicle_emissions(con: duckdb.DuckDBPyConnection) -> None:
    """
    Load `data/vehicle_emissions.csv` into a simple lookup table.

    Assumptions (from your file):
      - columns: vehicle_type, co2_grams_per_mile
      - we normalize vehicle_type -> taxi_type in {'yellow','green'}
    """
    logger.info("Loading vehicle_emissions from data/vehicle_emissions.csv")
    con.execute("DROP TABLE IF EXISTS vehicle_emissions;")
    con.execute("""
        CREATE TABLE vehicle_emissions AS
        SELECT
            CASE
                WHEN lower(vehicle_type) LIKE '%yellow%' THEN 'yellow'
                WHEN lower(vehicle_type) LIKE '%green%'  THEN 'green'
                ELSE lower(vehicle_type)
            END AS taxi_type,
            CAST(co2_grams_per_mile AS DOUBLE) AS co2_grams_per_mile
        FROM read_csv_auto('data/vehicle_emissions.csv', header=true);
    """)
    logger.info("vehicle_emissions loaded")


# ------------------------------------------------------------------------------
# Basic summarization (just raw row counts)
# ------------------------------------------------------------------------------
def summarize(con: duckdb.DuckDBPyConnection) -> None:
    """
    Print/log raw row counts for each table (before any cleaning).
    """
    y = con.execute("SELECT COUNT(*) FROM raw_yellow_all;").fetchone()[0]
    g = con.execute("SELECT COUNT(*) FROM raw_green_all;").fetchone()[0]
    e = con.execute("SELECT COUNT(*) FROM vehicle_emissions;").fetchone()[0]

    logger.info(f"raw_yellow_all rows: {y:,}")
    logger.info(f"raw_green_all rows:  {g:,}")
    logger.info(f"vehicle_emissions rows: {e:,}")

    print(f"raw_yellow_all rows: {y:,}")
    print(f"raw_green_all rows:  {g:,}")
    print(f"vehicle_emissions rows: {e:,}")


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
def main() -> None:
    con = None
    try:
        con = _connect()
        logger.info("Connected to DuckDB instance")

        # 1) Yellow trips 2015–2024
        load_yellow(con)

        # 2) Green trips 2015–2024
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
