# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "466e2184-c19b-4669-a1ea-0398d3683846",
# META       "default_lakehouse_name": "Vscode",
# META       "default_lakehouse_workspace_id": "36952dd4-29ea-4c9a-85fa-b35583ab33cc",
# META       "known_lakehouses": [
# META         {
# META           "id": "466e2184-c19b-4669-a1ea-0398d3683846"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS fact_error_event (
# MAGIC     table_name STRING,
# MAGIC     error_timestamp TIMESTAMP,
# MAGIC     error_type STRING,
# MAGIC     severity STRING,
# MAGIC     columns_checked STRING,
# MAGIC     row_identifier STRING,
# MAGIC     error_description STRING,
# MAGIC     collision_id STRING,
# MAGIC     extraction_timestamp string
# MAGIC ) USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lit, current_timestamp, concat_ws

# Step 1: Extract null rows from nyc_crashes
null_rows = spark.sql("""
    SELECT * FROM nyc_crashes
    WHERE borough IS NULL
""").withColumn("columns_checked", 
      lit("borough,location")
).withColumn("error_type", 
      lit("NULL_CHECK")
).withColumn("severity", 
      lit("HIGH")
).withColumn("error_description", 
      lit("Nulls found in borough or location")
).withColumn("error_timestamp", 
      current_timestamp()
).withColumn("table_name", 
      lit("nyc_crashes")
).withColumn("row_identifier", 
      concat_ws("-", *spark.table("nyc_crashes").columns)
)

# Step 2: Load existing error fact rows
existing_errors = spark.table("fact_error_event").filter(
    col("error_type") == "NULL_CHECK"
).select("row_identifier", "columns_checked", "error_type")

# Step 3: Anti-join to exclude already logged issues
new_errors = null_rows.join(existing_errors, 
    on=["row_identifier", "columns_checked", "error_type"], 
    how="left_anti"
).select(
    "table_name", 
    "error_timestamp", 
    "error_type", 
    "severity",
    "columns_checked", 
    "row_identifier", 
    "error_description", 
    "collision_id",
    'extraction_timestamp'  # include this field
)

# Step 4: Append only new error rows
if new_errors.count() > 0:
    inserted_count = new_errors.count()    
    new_errors.write.mode("append").format("delta").saveAsTable("fact_error_event")
    print(f"Number of nulls records inserted: {inserted_count}")
else:
    print('No new errors identified')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lit, current_timestamp, concat_ws

# Step 1: Define columns to check for duplicates
duplicate_check_cols = ["crash_date", "borough", "vehicle_type_code1", "collision_id", "latitude", "longitude", "crash_time"]

# Step 2: Identify duplicate keys
duplicate_keys = (
    spark.table("nyc_crashes")
    .groupBy(duplicate_check_cols)
    .count()
    .filter(col("count") > 1)
    .drop("count")
)


# Ensure extraction_timestamp is present and properly typed
if "extraction_timestamp" not in null_rows.columns:
    null_rows = null_rows.withColumn("extraction_timestamp", lit(None).cast(TimestampType()))
else:
    null_rows = null_rows.withColumn("extraction_timestamp", col("extraction_timestamp").cast(TimestampType()))


# Step 3: Get full duplicate rows
duplicate_rows = (
    spark.table("nyc_crashes")
    .join(duplicate_keys, on=duplicate_check_cols, how="inner")
    .withColumn("columns_checked", lit(",".join(duplicate_check_cols)))
    .withColumn("error_type", lit("DUPLICATE_CHECK"))
    .withColumn("severity", lit("MEDIUM"))
    .withColumn("error_description", lit("Duplicate record found"))
    .withColumn("error_timestamp", current_timestamp())
    .withColumn("table_name", lit("nyc_crashes"))
    .withColumn("row_identifier", concat_ws("-", *duplicate_check_cols))
)

# Step 4: Load existing duplicate error logs
existing_duplicates = spark.table("fact_error_event").filter(
    col("error_type") == "DUPLICATE_CHECK"
).select("row_identifier", "columns_checked", "error_type")

# Step 5: Filter out already logged duplicates
new_duplicates = duplicate_rows.join(
    existing_duplicates,
    on=["row_identifier", "columns_checked", "error_type"],
    how="left_anti"
).select(
    "table_name",
    "error_timestamp",
    "error_type",
    "severity",
    "columns_checked",
    "row_identifier",
    "error_description",
    "collision_id",
    "extraction_timestamp"  # ensure collision_id is included
)

# Step 6: Write new duplicate errors to fact table
if new_duplicates.count() > 0:
    inserted_count = duplicate_rows.count()
    new_duplicates.write.mode("append").format("delta").saveAsTable("fact_error_event")
    print(f"Number of duplicate records inserted: {inserted_count}")
else:
    print('No new duplicate errors found')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Vscode.fact_error_event")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
## checking the error distribution and error event timestamp


error_df =df.groupBy('error_timestamp',
    'error_type', 
    to_date(col("extraction_timestamp")).alias('extraction_date')
    ).agg(count('*').alias('count'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

error_df.write.mode('overwrite').format('delta').saveAsTable('Error_Counts')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## reading the tables from the lakehouse
bronze_df = spark.table("nyc_crashes")
error_ids_df = spark.table("fact_error_event").select("collision_id").distinct()

 ## the left anti join collects everything from the  and left that is not on the right table, hence if succesful there will be no nulls or duplicate

clean_df = bronze_df.join(error_ids_df, on="collision_id", how="left_anti")

## creating a new table name silver_nyc_crashes enforcing the 3 layer method (bronze, silver and gold layers)
clean_df.write.mode("overwrite").format("delta").saveAsTable("silver_nyc_crashes")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Checking if there is any nulls or duplicate using the fact error event table as reference
# MAGIC  
# MAGIC SELECT * FROM silver_nyc_crashes
# MAGIC JOIN fact_error_event ON silver_nyc_crashes.collision_id = fact_error_event.collision_id 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM error_counts

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM silver_nyc_crashes

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
