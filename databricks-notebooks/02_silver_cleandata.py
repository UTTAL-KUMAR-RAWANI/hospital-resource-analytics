from pyspark.sql.types import *
from pyspark.sql.functions import *

# ADLS configuration (same as Bronze notebook)
spark.conf.set(
  "fs.azure.account.key.<<STORAGE ACCOUNT NAME>>.dfs.core.windows.net",
  dbutils.secrets.get(scope="<<vaultscope>>", key="<<connection-key>>")
)

# Paths
bronze_path = "abfss://<<CONTAINER>>@<<STORAGE ACCOUNT NAME>>.dfs.core.windows.net/<<path>>"
silver_path = "abfss://<<CONTAINER>>@<<STORAGE ACCOUNT NAME>>.dfs.core.windows.net/<<path>>"
checkpoint_path = "abfss://<<CONTAINER>>@<<STORAGE ACCOUNT NAME>>.dfs.core.windows.net/_checkpoints/<<path>>"

# Read from Bronze Delta table
bronze_df = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
)

# Define schema
schema = StructType([
    StructField("patient_id", StringType()),
    StructField("gender", StringType()),
    StructField("age", IntegerType()),
    StructField("department", StringType()),
    StructField("admission_time", StringType()),
    StructField("discharge_time", StringType()),
    StructField("bed_id", IntegerType()),
    StructField("hospital_id", IntegerType())
])

# Parse JSON
parsed_df = (
    bronze_df
    .withColumn("data", from_json(col("raw_json"), schema))
    .select("data.*")
)

# Convert to timestamp
clean_df = parsed_df.withColumn(
    "admission_time", to_timestamp("admission_time")
).withColumn(
    "discharge_time", to_timestamp("discharge_time")
)

# Handle invalid admission_time
clean_df = clean_df.withColumn(
    "admission_time",
    when(
        col("admission_time").isNull() | (col("admission_time") > current_timestamp()),
        current_timestamp()
    ).otherwise(col("admission_time"))
)

# Handle invalid age
clean_df = clean_df.withColumn(
    "age",
    when(col("age") > 100, floor(rand()*90 + 1).cast("int"))
    .otherwise(col("age"))
)

# Schema evolution handling
expected_cols = [
    "patient_id",
    "gender",
    "age",
    "department",
    "admission_time",
    "discharge_time",
    "bed_id",
    "hospital_id"
]

for col_name in expected_cols:
    if col_name not in clean_df.columns:
        clean_df = clean_df.withColumn(col_name, lit(None))

# Write to Silver Delta table
(
    clean_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("mergeSchema", "true")
    .option("checkpointLocation", checkpoint_path)
    .start(silver_path)
)