#!/usr/bin/env python3

# After many tries, I did not end up dropping all duplicates because my program crashed every time due to insufficient disk space

# Importing libraries
import duckdb
import logging
import os


# Setting up constants and environment variables

DB_PATH = os.environ.get("DB_PATH", "emissions.duckdb")
YEAR_MIN, YEAR_MAX = 2015, 2024  

# Tuning to try to reduce usage, but was still unable to get drop duplicates to work
DUCKDB_TEMP_DIR   = os.environ.get("DUCKDB_TEMP_DIR", "")     
MAX_TEMP_DIR_SIZE = os.environ.get("MAX_TEMP_DIR_SIZE", "50GiB") 
MEMORY_LIMIT      = os.environ.get("MEMORY_LIMIT", "8GB")        
THREADS           = os.environ.get("DUCKDB_THREADS", "4")         

# Raw (from load.py)
YELLOW_RAW = "raw_yellow_all"
GREEN_RAW  = "raw_green_all"

# Cleaned outputs
YELLOW_CLEAN = "yellow_clean"
GREEN_CLEAN  = "green_clean"


# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="clean.log"
)
logger = logging.getLogger(__name__)

# Connect to DuckDB with performance pragmas
def _connect() -> duckdb.DuckDBPyConnection:
    logger.info("Opening DuckDB connection")
    con = duckdb.connect(database=DB_PATH, read_only=False)
    logger.info("DuckDB connection established")

    # Tuning: reduce temp-pressure and allow larger spill
    logger.info(
        "Applying performance pragmas: "
        f"threads={THREADS}, memory_limit={MEMORY_LIMIT}, "
        f"max_temp_directory_size={MAX_TEMP_DIR_SIZE}, temp_directory={DUCKDB_TEMP_DIR or '(default)'}"
    )
    con.execute(f"SET threads={THREADS};")
    con.execute("SET preserve_insertion_order=false;")
    con.execute(f"PRAGMA memory_limit='{MEMORY_LIMIT}';")
    con.execute(f"PRAGMA max_temp_directory_size='{MAX_TEMP_DIR_SIZE}';")
    if DUCKDB_TEMP_DIR:
        os.makedirs(DUCKDB_TEMP_DIR, exist_ok=True)
        con.execute(f"PRAGMA temp_directory='{DUCKDB_TEMP_DIR}';")
    return con

# Defining columns and row counting helper
def _pickup_drop_cols(table_name: str):
    return ("tpep_pickup_datetime", "tpep_dropoff_datetime") if "yellow" in table_name \
           else ("lpep_pickup_datetime", "lpep_dropoff_datetime")

def _rows(con: duckdb.DuckDBPyConnection, tbl: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]

# 
# Cleaning using con.execute steps with detailed progress logging

def clean_one(con: duckdb.DuckDBPyConnection, src: str, dest: str):
    pick, drop = _pickup_drop_cols(src)

    logger.info(f"BEGIN cleaning for source={src}, dest={dest}")
    print(f"{dest}: begin cleaning from {src}")

    # Starting from raw 
    logger.info(f"Dropping existing table {dest} if it exists")
    con.execute(f"DROP TABLE IF EXISTS {dest};")

    logger.info(f"Creating working copy {dest} from {src}")
    con.execute(f"CREATE TABLE {dest} AS SELECT * FROM {src};")
    start_cnt = _rows(con, dest)
    logger.info(f"{dest}: working copy created with {start_cnt:,} rows")
    print(f"{dest}: starting rows = {start_cnt:,}")
    con.execute("PRAGMA force_checkpoint;")
    logger.info("Checkpoint after working copy")

    # Removing rows with years outside [2015, 2024] on BOTH pickup & dropoff
    logger.info("Step 1/6: Computing out-of-range year rows")
    bad_years = con.execute(f"""
        SELECT COUNT(*) FROM {dest}
        WHERE NOT (
          EXTRACT(YEAR FROM {pick}) BETWEEN {YEAR_MIN} AND {YEAR_MAX}
          AND EXTRACT(YEAR FROM {drop}) BETWEEN {YEAR_MIN} AND {YEAR_MAX}
        );
    """).fetchone()[0]
    logger.info(f"Step 1/6: Out-of-range year rows to remove: {bad_years:,}")
    con.execute(f"""
        DELETE FROM {dest}
        WHERE NOT (
          EXTRACT(YEAR FROM {pick}) BETWEEN {YEAR_MIN} AND {YEAR_MAX}
          AND EXTRACT(YEAR FROM {drop}) BETWEEN {YEAR_MIN} AND {YEAR_MAX}
        );
    """)
    rem_cnt = _rows(con, dest)
    logger.info(f"Step 1/6 complete: {dest} now has {rem_cnt:,} rows")
    print(f"{dest}: removed out-of-range years = {bad_years:,}")
    con.execute("PRAGMA force_checkpoint;")
    logger.info("Checkpoint after step 1")

    # Removing trips with 0 passengers
    logger.info("Step 2/6: Computing passenger_count = 0 rows")
    zero_pax = con.execute(f"""
        SELECT COUNT(*) FROM {dest}
        WHERE COALESCE(passenger_count, 0) = 0;
    """).fetchone()[0]
    logger.info(f"Step 2/6: passenger_count=0 rows to remove: {zero_pax:,}")
    con.execute(f"""
        DELETE FROM {dest}
        WHERE COALESCE(passenger_count, 0) = 0;
    """)
    rem_cnt = _rows(con, dest)
    logger.info(f"Step 2/6 complete: {dest} now has {rem_cnt:,} rows")
    print(f"{dest}: removed passenger_count = 0 = {zero_pax:,}")
    con.execute("PRAGMA force_checkpoint;")
    logger.info("Checkpoint after step 2")

    # Removing trips with 0 miles or fewer
    logger.info("Step 3/6: Computing trip_distance <= 0 rows")
    dist_le_zero = con.execute(f"""
        SELECT COUNT(*) FROM {dest}
        WHERE trip_distance <= 0;
    """).fetchone()[0]
    logger.info(f"Step 3/6: trip_distance <= 0 rows to remove: {dist_le_zero:,}")
    con.execute(f"DELETE FROM {dest} WHERE trip_distance <= 0;")
    rem_cnt = _rows(con, dest)
    logger.info(f"Step 3/6 complete: {dest} now has {rem_cnt:,} rows")
    print(f"{dest}: removed trip_distance <= 0 = {dist_le_zero:,}")
    con.execute("PRAGMA force_checkpoint;")
    logger.info("Checkpoint after step 3")

    # Remove trips with distance > 100 miles
    logger.info("Step 4/6: Computing trip_distance > 100 rows")
    dist_gt_100 = con.execute(f"""
        SELECT COUNT(*) FROM {dest}
        WHERE trip_distance > 100;
    """).fetchone()[0]
    logger.info(f"Step 4/6: trip_distance > 100 rows to remove: {dist_gt_100:,}")
    con.execute(f"DELETE FROM {dest} WHERE trip_distance > 100;")
    rem_cnt = _rows(con, dest)
    logger.info(f"Step 4/6 complete: {dest} now has {rem_cnt:,} rows")
    print(f"{dest}: removed trip_distance > 100 = {dist_gt_100:,}")
    con.execute("PRAGMA force_checkpoint;")
    logger.info("Checkpoint after step 4")

    # Removing trips with negative duration
    logger.info("Step 5/6: Computing negative-duration rows")
    neg_duration = con.execute(f"""
        SELECT COUNT(*) FROM {dest}
        WHERE {drop} < {pick};
    """).fetchone()[0]
    logger.info(f"Step 5/6: negative-duration rows to remove: {neg_duration:,}")
    con.execute(f"""
        DELETE FROM {dest}
        WHERE {drop} < {pick};
    """)
    rem_cnt = _rows(con, dest)
    logger.info(f"Step 5/6 complete: {dest} now has {rem_cnt:,} rows")
    print(f"{dest}: removed negative duration = {neg_duration:,}")
    con.execute("PRAGMA force_checkpoint;")
    logger.info("Checkpoint after step 5")

    # Removing trips lasting more than 1 day
    logger.info("Step 6/6: Computing over-24h rows")
    over_24h = con.execute(f"""
        SELECT COUNT(*) FROM {dest}
        WHERE ({drop} - {pick}) > INTERVAL 1 DAY;
    """).fetchone()[0]
    logger.info(f"Step 6/6: over-24h rows to remove: {over_24h:,}")
    con.execute(f"""
        DELETE FROM {dest}
        WHERE ({drop} - {pick}) > INTERVAL 1 DAY;
    """)
    rem_cnt = _rows(con, dest)
    logger.info(f"Step 6/6 complete: {dest} now has {rem_cnt:,} rows")
    print(f"{dest}: removed duration > 24h = {over_24h:,}")
    con.execute("PRAGMA force_checkpoint;")
    logger.info("Checkpoint after step 6")

    # Final count
    final_cnt = _rows(con, dest)
    print(f"{dest}: final rows = {final_cnt:,}")
    logger.info(f"{dest}: final row count = {final_cnt:,}")
    logger.info(f"END cleaning for dest={dest}")

# Verification (full-table checks for all rules except duplicates)

def verify_one(con: duckdb.DuckDBPyConnection, tbl: str, is_yellow: bool):
    pick, drop = ("tpep_pickup_datetime", "tpep_dropoff_datetime") if is_yellow \
                 else ("lpep_pickup_datetime", "lpep_dropoff_datetime")

    logger.info(f"BEGIN verification for table={tbl}")
    print(f"\nVerification checklist for {tbl}:")
    logger.info(f"Verification checklist for {tbl}:")

    checks = [
        ("passenger_count = 0", f"SELECT COUNT(*) FROM {tbl} WHERE COALESCE(passenger_count, 0) = 0"),
        ("trip_distance <= 0", f"SELECT COUNT(*) FROM {tbl} WHERE trip_distance <= 0"),
        ("trip_distance > 100", f"SELECT COUNT(*) FROM {tbl} WHERE trip_distance > 100"),
        ("negative duration", f"SELECT COUNT(*) FROM {tbl} WHERE {drop} < {pick}"),
        ("duration > 24h", f"SELECT COUNT(*) FROM {tbl} WHERE ({drop} - {pick}) > INTERVAL 1 DAY"),
        (f"year outside [{YEAR_MIN},{YEAR_MAX}]", f"""
            SELECT COUNT(*) FROM {tbl}
            WHERE NOT (
              EXTRACT(YEAR FROM {pick}) BETWEEN {YEAR_MIN} AND {YEAR_MAX}
              AND EXTRACT(YEAR FROM {drop}) BETWEEN {YEAR_MIN} AND {YEAR_MAX}
            )
        """),
    ]

    all_ok = True
    for label, sql in checks:
        count = con.execute(sql).fetchone()[0]
        line = f"  {label}: {count}"
        print(line)
        logger.info(line)
        if count != 0:
            all_ok = False

    status = "CLEAN PASSED" if all_ok else "CLEAN FAILED"
    print(status)
    logger.info(status)
    logger.info(f"END verification for table={tbl}")

# Main

def main():
    con = None
    try:
        con = _connect()
        logger.info(f"Connected to DuckDB at {DB_PATH}")

        print("\nCleaning YELLOW")
        logger.info("Starting cleaning for YELLOW")
        clean_one(con, YELLOW_RAW, YELLOW_CLEAN)

        print("\nCleaning GREEN")
        logger.info("Starting cleaning for GREEN")
        clean_one(con, GREEN_RAW, GREEN_CLEAN)

        # Summary (post-clean counts)
        y_cnt = _rows(con, YELLOW_CLEAN)
        g_cnt = _rows(con, GREEN_CLEAN)
        summary = (
            f"\nSummary — rows after cleaning: "
            f"yellow_clean={y_cnt:,}, green_clean={g_cnt:,}"
        )
        print(summary)
        logger.info(summary)

        # Verification checklists (screen + log)
        verify_one(con, YELLOW_CLEAN, is_yellow=True)
        verify_one(con, GREEN_CLEAN,  is_yellow=False)

        # Replace raw with clean and compact the file so it shrinks
        print("\nDropping raw tables to keep only cleaned data…")
        logger.info("Dropping raw tables raw_yellow_all, raw_green_all")
        con.execute("DROP TABLE IF EXISTS raw_yellow_all;")
        con.execute("DROP TABLE IF EXISTS raw_green_all;")
        con.execute("PRAGMA force_checkpoint;")
        logger.info("Raw tables dropped; running VACUUM to compact file")

        print("Running VACUUM to compact emissions.duckdb …")
        con.execute("VACUUM;") 
        logger.info("VACUUM complete")

        print("Cleanup complete. Database now contains only yellow_clean and green_clean (plus any lookups).")
        logger.info("Cleaning stage completed, raw tables removed, database compacted")
    except Exception as e:
        logger.exception(f"Cleaning error: {e}")
        raise
    finally:
        if con:
            con.close()
            logger.info("Closed DuckDB connection")

if __name__ == "__main__":
    main()
