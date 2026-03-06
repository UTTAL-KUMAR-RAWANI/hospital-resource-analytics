from pyspark.sql.functions import *

# Azure Event Hub Configuration
event_hub_namespace = "<<EVENTHUB NAMESPACE>>"
event_hub_name="<<EVENTHUB NAME>>"  
event_hub_conn_str = dbutils.secrets.get(scope="<<vaultscope>>", key="<<connection-key>>")

kafka_options = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}
#Read from eventhub
raw_df = (spark.readStream
          .format("kafka")
          .options(**kafka_options)
          .load()
          )

#Cast data to json
json_df = raw_df.selectExpr("CAST(value AS STRING) as raw_json")

#ADLS configuration 
spark.conf.set(
  "fs.azure.account.key.<<STORAGE ACCOUNT NAME>>.dfs.core.windows.net",
  dbutils.secrets.get(scope="<<vaultscope>>", key="<<connection-key>>")
)

bronze_path = "abfss://<<CONTAINER>>@<<STORAGE ACCOUNT NAME>>.dfs.core.windows.net/<<path>>"
checkpoint_path = "abfss://<<CONTAINER>>e@<<STORAGE ACCOUNT NAME>>.dfs.core.windows.net/_checkpoints/<<path>>"

#Write stream to bronze
(
    json_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .start(bronze_path)
)