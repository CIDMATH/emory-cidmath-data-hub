"""
Process MDCD files to Delta tables.

Pattern: /Volumes/.../MDCD/2014 v20/PARQUET/medicaid_a_2014_v20_0_0_0.snappy.parquet
Table:   raw_marketscan_mdcda_2014_v20

Grouping: mdcd + table character (position 9 in filename) + year/version
Table character: Character after 'medicaid_' (e.g., 'a' in 'medicaid_a')
"""
from pyspark.sql import SparkSession
from collections import defaultdict
import re

spark = SparkSession.builder.getOrCreate()

# --- Configuration ---
CATALOG_NAME = "ecdh"
SCHEMA_NAME = "marketscan"
VOLUME_NAME = "marketscan"
DATA_SOURCE = "MDCD"

VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}/{DATA_SOURCE}"


def list_parquet_files(path: str) -> list[str]:
    """Recursively list all .parquet files."""
    results = []
    for item in dbutils.fs.ls(path):
        if item.name.endswith("/"):
            results.extend(list_parquet_files(item.path))
        elif item.name.endswith(".parquet"):
            results.append(item.path)
    return results


def derive_table_name(file_path: str) -> str:
    """
    Extract table name from MDCD file path.
    
    Example:
        /MDCD/2014 v20/PARQUET/medicaid_a_2014_v20_0_0_0.snappy.parquet
        → raw_marketscan_mdcda_2014_v20
        
    Table code: 'mdcd' + character at position 9 (e.g., 'a' -> 'mdcda')
    """
    parts = file_path.split("/")
    
    file_name = parts[-1]
    if file_name.endswith(".snappy.parquet"):
        base_name = file_name.replace(".snappy.parquet", "")
    else:
        base_name = file_name.replace(".parquet", "")
    
    if len(base_name) < 10:
        raise ValueError(f"MDCD filename too short: {file_name}")
    
    # Extract table character from position 9 (0-indexed)
    # Format: medicaid_a_... where 'a' is at position 9
    table_char = base_name[9]
    table_code = f"mdcd{table_char}"
    
    # Extract year and version from directory path
    year = None
    version = None
    for part in parts:
        match = re.match(r'^(\d{4})\s+v(\d+)$', part)
        if match:
            year = match.group(1)
            version = f"v{match.group(2)}"
            break
    
    if not year or not version:
        raise ValueError(f"Missing year/version in path: {file_path}")
    
    table_name = f"raw_marketscan_{table_code}_{year}_{version}"
    return table_name.lower()


def get_file_prefix(file_path: str) -> str:
    """
    Create grouping key: table_code|year_version
    
    Example: mdcda|2014_v20
    """
    parts = file_path.split("/")
    
    file_name = parts[-1]
    if file_name.endswith(".snappy.parquet"):
        base_name = file_name.replace(".snappy.parquet", "")
    else:
        base_name = file_name.replace(".parquet", "")
    
    if len(base_name) >= 10:
        table_char = base_name[9]
        table_code = f"mdcd{table_char}"
    else:
        table_code = "mdcd"
    
    # Extract year/version from directory path
    year_version = None
    for part in parts:
        match = re.match(r'^(\d{4})\s+v(\d+)$', part)
        if match:
            year_version = f"{match.group(1)}_v{match.group(2)}"
            break
    
    key_parts = [table_code]
    if year_version:
        key_parts.append(year_version)
    
    return "|".join(key_parts)


def main():
    print(f"Processing {DATA_SOURCE}")
    print(f"Scanning volume: {VOLUME_PATH}\n")

    parquet_files = list_parquet_files(VOLUME_PATH)

    if not parquet_files:
        print("No parquet files found. Exiting.")
        return

    print(f"Found {len(parquet_files)} parquet file(s)\n")

    # Group files by prefix
    file_groups = defaultdict(list)
    for file_path in parquet_files:
        prefix = get_file_prefix(file_path)
        file_groups[prefix].append(file_path)

    print(f"Grouped into {len(file_groups)} unique table(s)\n")

    # Dry-run preview
    print("DRY-RUN: Previewing file groups -> table name mapping")
    print("=" * 70)
    
    for prefix, files in sorted(file_groups.items()):
        table_name = derive_table_name(files[0])
        full_table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{table_name}"
        
        print(f"\nTable: {full_table_name}")
        print(f"  Prefix: {prefix}")
        print(f"  Files ({len(files)}):")
        for file_path in sorted(files):
            file_name = file_path.split("/")[-1]
            print(f"    - {file_name}")
    
    print("\n" + "=" * 70)

    # Uncomment to stop after dry run
    # return

    print("\nProceeding with table creation...\n")

    skipped = 0
    created = 0
    errors = 0

    for prefix, files in sorted(file_groups.items()):
        table_name = derive_table_name(files[0])
        full_table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{table_name}"

        print(f"\n{'=' * 70}")
        print(f"Table  : {full_table_name}")
        print(f"Prefix : {prefix}")
        print(f"Files  : {len(files)} file(s)")

        if spark.catalog.tableExists(full_table_name):
            print(f"Status : SKIPPED (table already exists)")
            skipped += 1
            continue

        try:
            # Read and union all files
            dfs = []
            for file_path in sorted(files):
                file_name = file_path.split("/")[-1]
                print(f"  Reading: {file_name}")
                df = spark.read.parquet(file_path)
                dfs.append(df)

            if len(dfs) == 1:
                merged_df = dfs[0]
            else:
                from functools import reduce
                merged_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)

            # Write Delta table
            (
                merged_df.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(full_table_name)
            )

            row_count = merged_df.count()
            print(f"Status : CREATED ({row_count} rows from {len(files)} file(s))")
            created += 1

        except Exception as e:
            print(f"Status : FAILED — {e}")
            errors += 1

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  Total files found      : {len(parquet_files)}")
    print(f"  Unique table groups    : {len(file_groups)}")
    print(f"  Tables created         : {created}")
    print(f"  Tables skipped         : {skipped}")
    print(f"  Errors                 : {errors}")
    print("=" * 70)


if __name__ == "__main__":
    main()
