# Databricks notebook source
"""
Combine the tables by year
e.g. 2014-2025 tables for each service type
"""

###############################
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import re

# Initialize Spark session (if not already available)
spark = SparkSession.builder.appName("CombineTables").getOrCreate()

# Get all table names from the database
# --- Configuration ---
CATALOG_NAME = "ecdh"
SCHEMA_NAME = "marketscan"
# TEST_TABLE_PREFIX = "raw_marketscan_set_a_ccaei"  # Only process tables starting with this

# For listTables, use catalog.schema format (not Volumes path)
database_name = f"{CATALOG_NAME}.{SCHEMA_NAME}"
all_tables = spark.catalog.listTables(database_name)

"""
# Filter to only test tables (set_a_ccaei)
all_tables = [t for t in all_tables if t.name.startswith(TEST_TABLE_PREFIX)]

print(f"Found {len(all_tables)} tables matching '{TEST_TABLE_PREFIX}':")
for table in all_tables:
    print(f"  - {table.name}")
"""

# Group tables by their base name (everything before yyyy)
table_groups = {}
pattern = r'^(.+?)_(\d{4})_v\d+$'

for table in all_tables:
    table_name = table.name
    match = re.match(pattern, table_name)
    
    if match:
        base_name = match.group(1)  # e.g., "raw_marketscan_set_a_ccaea"
        year = match.group(2)        # e.g., "2014"
        
        if base_name not in table_groups:
            table_groups[base_name] = []
        
        table_groups[base_name].append({
            'full_name': table_name,
            'year': year
        })

# Combine tables for each group
combined_tables = {}

for base_name, tables in table_groups.items():
    print(f"Processing group: {base_name}")
    
    dataframes = []
    
    for table_info in tables:
        table_name = table_info['full_name']
        year = table_info['year']
        
        # Read the table and add year column
        df = spark.table(f"{database_name}.{table_name}")
        df_with_year = df.withColumn("year", lit(year))
        
        dataframes.append(df_with_year)
        print(f"  Added {table_name} with year {year}")
    
    # Union all dataframes in the group
    if dataframes:
        combined_df = dataframes[0]
        for df in dataframes[1:]:
            combined_df = combined_df.union(df)
        
        combined_tables[base_name] = combined_df
        print(f"  Combined {len(dataframes)} tables for {base_name}")

# Access your combined tables
"""
# For example, for raw_marketscan_set_a_ccaea:
if 'raw_marketscan_set_a_ccaea' in combined_tables:
   result_df = combined_tables['raw_marketscan_set_a_ccaea']
   result_df.show()
    
    # Optionally save to a new table
    # result_df.write.mode("overwrite").saveAsTable(f"{database_name}.combined_raw_marketscan_set_a_ccaea")

if 'raw_marketscan_set_a_ccaei' in combined_tables:
    result_df = combined_tables['raw_marketscan_set_a_ccaei']
    
    # Show basic info
    print(f"\nTotal rows: {result_df.count():,}")
    
    # Show sample data
    print("\nSample data:")
    display(result_df.limit(100))  # Use display() in Databricks notebooks
    
    # Show year distribution
    print("\nYear distribution:")
    display(result_df.groupBy("year").count().orderBy("year"))
"""