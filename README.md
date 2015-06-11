Running the Ingester
====================

Parameters:
java -jar kafka-parquet-ingest-0.1.jar zkhost:port groupID /path/to/avro/schema.avsc topic hdfs://nn.fqdn:8020/path/in/hdfs blocksize idleTimeoutInSeconds

Example:
java -jar kafka-parquet-ingest-0.1.jar 192.168.22.100:2181 kafka-parquet-ingest /schemas/event.avsc events hdfs://dev.local:8020/data/events 268435456 30


