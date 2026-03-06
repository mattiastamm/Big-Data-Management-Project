from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# ---------- Paths ----------
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
INBOX_DIR = DATA_DIR / "inbox"
STATE_DIR = BASE_DIR / "state"
MANIFEST_PATH = STATE_DIR / "manifest.json"
LOOKUP_PATH = DATA_DIR / "taxi_zone_lookup.parquet"



# ===================================================================
# ===================================================================
#                         EXTRACTION PHASE
# ===================================================================
# ===================================================================

def load_manifest(manifest_path: Path) -> dict[str, Any]:
    """
    Load the manifest file if it exists.
    If it does not exist, return an empty manifest structure.
    """
    if not manifest_path.exists():
        return {"processed_files": []}

    with manifest_path.open("r", encoding="utf-8") as f:
        manifest = json.load(f)

    # Defensive fallback in case the file exists but is missing the key
    if "processed_files" not in manifest:
        manifest["processed_files"] = []

    return manifest


def list_inbox_files(inbox_dir: Path) -> list[Path]:
    """
    Return all parquet files currently present in the inbox directory.
    Sorted by filename for deterministic behavior.
    """
    if not inbox_dir.exists():
        return []

    return sorted(inbox_dir.glob("*.parquet"))


def detect_new_files(inbox_files: list[Path], manifest: dict[str, Any], ) -> list[Path]:
    """
    Compare inbox files against the manifest and return only files
    that have not yet been processed.
    """
    processed_filenames = {
        entry["filename"]
        for entry in manifest.get("processed_files", [])
        if "filename" in entry
    }

    new_files = [
        file_path
        for file_path in inbox_files
        if file_path.name not in processed_filenames
    ]

    return new_files


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Incremental Taxi ETL")
        .master("local[*]")
        .getOrCreate()
    )


from datetime import datetime, timezone


def extract_new_data(spark: SparkSession, new_files: list[Path], ingestion_ts: datetime, ) -> DataFrame | None:
    """
    Read only the new parquet files and add required metadata columns.
    Returns None if there are no new files.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    new_files : list[Path]
        New parquet files to process.
    ingestion_ts : datetime
        One fixed timestamp for the whole pipeline run.
    """
    if not new_files:
        return None

    print("\n=== EXTRACTION STAGE ===")
    print(f"Reading {len(new_files)} new file(s):")
    for path in new_files:
        print(f"  - {path.name}")

    file_paths = [str(path) for path in new_files]

    df = spark.read.parquet(*file_paths)

    df = df.withColumn(
        "source_file",
        F.element_at(F.split(F.input_file_name(), r"[/\\]"), -1),
    )
    df = df.withColumn("ingested_at", F.lit(ingestion_ts))

    print("\nSchema of extracted data:")
    df.printSchema()

    print("\nSample rows:")
    df.show(5, truncate=False)

    print(f"\nExtracted row count: {df.count():,}")

    return df



# ===================================================================
# ===================================================================
#                       TRANSFORMATION PHASE
# ===================================================================
# ===================================================================

REQUIRED_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount",
    "source_file",
    "ingested_at",
]


DEDUP_KEY = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "fare_amount",
]


def select_and_cast_columns(df: DataFrame) -> DataFrame:
    """
    Select required columns and cast them to the expected types.
    """

    df = df.select(*REQUIRED_COLUMNS)

    df = df.select(
        F.col("tpep_pickup_datetime").cast("timestamp").alias("tpep_pickup_datetime"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("tpep_dropoff_datetime"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("PULocationID").cast("int").alias("PULocationID"),
        F.col("DOLocationID").cast("int").alias("DOLocationID"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("source_file"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )

    return df


def clean_trips(df: DataFrame) -> DataFrame:
    print("\n=== CLEANING STAGE ===")

    initial_count = df.count()
    count_before = initial_count
    print(f"Rows before cleaning: {count_before:,}")

    # 1) Remove rows with missing timestamps
    df = df.filter(
        F.col("tpep_pickup_datetime").isNotNull() &
        F.col("tpep_dropoff_datetime").isNotNull()
    )
    count_after = df.count()
    print(f"After removing null timestamps: {count_after:,} "
          f"(removed {count_before - count_after:,})")
    count_before = count_after

    # 2) Remove rows where dropoff is not after pickup
    df = df.filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    count_after = df.count()
    print(f"After removing invalid timestamp order: {count_after:,} "
          f"(removed {count_before - count_after:,})")
    count_before = count_after

    # 3) Remove trips longer than 24 hours
    df = df.filter(
        (
            F.unix_timestamp("tpep_dropoff_datetime") -
            F.unix_timestamp("tpep_pickup_datetime")
        ) <= 24 * 60 * 60
    )
    count_after = df.count()
    print(f"After removing trips longer than 24 hours: {count_after:,} "
          f"(removed {count_before - count_after:,})")
    count_before = count_after

    # 4) Remove non-positive trip distances
    df = df.filter(F.col("trip_distance") > 0)
    count_after = df.count()
    print(f"After removing non-positive distances: {count_after:,} "
          f"(removed {count_before - count_after:,})")
    count_before = count_after

    # 5) Remove negative fares
    df = df.filter(F.col("fare_amount") >= 0)
    count_after = df.count()
    print(f"After removing negative fares: {count_after:,} "
          f"(removed {count_before - count_after:,})")
    count_before = count_after

    # 6) Remove negative total amounts
    df = df.filter(F.col("total_amount") >= 0)
    count_after = df.count()
    print(f"After removing negative total amounts: {count_after:,} "
          f"(removed {count_before - count_after:,})")
    count_before = count_after

    # 7) Remove negative passenger counts
    df = df.filter(
        F.col("passenger_count").isNull() | (F.col("passenger_count") >= 0)
    )
    count_after = df.count()
    print(f"After removing negative passenger counts: {count_after:,} "
          f"(removed {count_before - count_after:,})")
    count_before = count_after

    # 8) Remove invalid location IDs
    df = df.filter(
        (F.col("PULocationID") > 0) &
        (F.col("DOLocationID") > 0)
    )
    count_after = df.count()
    print(f"After removing invalid location IDs: {count_after:,} "
          f"(removed {count_before - count_after:,})")

    total_removed = initial_count - count_after
    print(f"\nTotal rows removed during cleaning: {total_removed:,}")
    print(f"Rows remaining after cleaning: {count_after:,}")

    return df


def deduplicate_trips(df: DataFrame) -> DataFrame:
    """
    Remove duplicate trips using a defined business key.
    """
    print("\n=== DEDUPLICATION STAGE ===")
    print("Deduplication key:", DEDUP_KEY)

    count_before = df.count()
    print(f"Rows before deduplication: {count_before:,}")

    df = df.dropDuplicates(DEDUP_KEY)

    count_after = df.count()
    print(f"Rows after deduplication: {count_after:,} "
          f"(removed {count_before - count_after:,})")

    return df


def add_derived_columns(df: DataFrame) -> DataFrame:
    """
    Add derived analytical columns.
    """
    print("\n=== FEATURE ENGINEERING STAGE ===")

    df = df.withColumn(
        "trip_duration_minutes",
        (
            F.unix_timestamp("tpep_dropoff_datetime")
            - F.unix_timestamp("tpep_pickup_datetime")
        ) / 60.0
    )

    df = df.withColumn(
        "pickup_date",
        F.to_date("tpep_pickup_datetime")
    )

    print("Added columns: trip_duration_minutes, pickup_date")

    return df



# ===================================================================
# ===================================================================
#                         ENRICHMENT PHASE
# ===================================================================
# ===================================================================

def enrich_with_zones(spark: SparkSession, df: DataFrame, lookup_path: Path) -> DataFrame:
    """
    Enrich trips with pickup and dropoff zone information
    using the taxi zone lookup table.
    """
    print("\n=== ENRICHMENT STAGE ===")

    zone_df = spark.read.parquet(str(lookup_path)).select(
        F.col("LocationID").cast("int").alias("LocationID"),
        F.col("Zone").alias("Zone"),
    )

    print("Zone lookup schema:")
    zone_df.printSchema()

    pickup_lookup = zone_df.select(
        F.col("LocationID").alias("pickup_LocationID"),
        F.col("Zone").alias("pickup_zone"),
    )

    dropoff_lookup = zone_df.select(
        F.col("LocationID").alias("dropoff_LocationID"),
        F.col("Zone").alias("dropoff_zone"),
    )

    df = df.join(
        F.broadcast(pickup_lookup),
        df["PULocationID"] == pickup_lookup["pickup_LocationID"],
        how="left",
    )

    df = df.join(
        F.broadcast(dropoff_lookup),
        df["DOLocationID"] == dropoff_lookup["dropoff_LocationID"],
        how="left",
    )

    df = df.drop("pickup_LocationID", "dropoff_LocationID")

    # Log enrichment results for safety
    print("Schema after enrichment:")
    df.printSchema()

    print("\nSample enriched rows:")
    df.select(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "pickup_zone",
        "DOLocationID",
        "dropoff_zone",
        "trip_distance",
        "fare_amount",
        "trip_duration_minutes",
        "pickup_date",
        "source_file",
        "ingested_at",
    ).show(5, truncate=False)

    return df



# ===================================================================
# ===================================================================
#                         MAIN FUNCTION
# ===================================================================
# ===================================================================

def main() -> None:
    manifest = load_manifest(MANIFEST_PATH)
    inbox_files = list_inbox_files(INBOX_DIR)
    new_files = detect_new_files(inbox_files, manifest)

    if not new_files:
        print("\nNo new files found. Exiting.")
        return

    spark = create_spark_session()
    ingestion_ts = datetime.now(timezone.utc)

    try:
        # ===== Extraction =====
        df = extract_new_data(spark, new_files, ingestion_ts)

        # ===== Transformation =====
        df = select_and_cast_columns(df)
        df = clean_trips(df)
        df = deduplicate_trips(df)
        df = add_derived_columns(df)

        # ===== Enrichment =====
        df = enrich_with_zones(spark, df, LOOKUP_PATH)
        print("\nFinal row count after enrichment:", df.count())
    
    except Exception as e:
        print("\nPipeline failed:", e)
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()