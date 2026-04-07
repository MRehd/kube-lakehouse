import os
from pyflink.table import StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment

class FlinkTransactionsLoader:

  jars = [
    "file:///opt/flink/lib/flink-connector-kafka-3.4.0-1.20.jar",
    "file:///opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar",
    "file:///opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"
  ]

  env = StreamExecutionEnvironment.get_execution_environment()
  env.enable_checkpointing(10000)
  t_env = StreamTableEnvironment.create(env)
  t_env.get_config().set("pipeline.jars", ";".join(jars))
  t_env.get_config().set("pipeline.classpaths", ";".join(jars))
  t_env.get_config().set("rest.address", os.getenv('FLINK_HOST'))
  t_env.get_config().set("rest.port", os.getenv('FLINK_PORT'))
  t_env.get_config().set("classloader.resolve-order", "parent-first")
  t_env.get_config().set("classloader.parent-first-patterns.additional", "com.codahale.metrics")
  t_env.get_config().set("state.backend", "filesystem")
  t_env.get_config().set("state.checkpoints.dir", "file:///flink-data/checkpoints")

  def __init__(self, catalog, schema, table, topic):
    self.catalog = catalog
    self.schema = schema
    self.table = table
    self.topic = topic

  def init_entity(self):
    # Register Iceberg + Nessie catalog
    self.t_env.execute_sql(
      f"""
      CREATE CATALOG IF NOT EXISTS {self.catalog} WITH (
        'type' = 'iceberg',
        'catalog-impl' = 'org.apache.iceberg.nessie.NessieCatalog',
        'uri' = '{os.getenv('NESSIE_URI')}',
        'nessie.auth.type' = 'none',
        'warehouse' = 's3a://{self.catalog}',
        'ref' = 'main',
        'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
        's3.endpoint' = '{os.getenv('AWS_S3_ENDPOINT')}',
        's3.path-style-access' = 'true',
        's3.access-key-id' = '{os.getenv('AWS_ACCESS_KEY_ID')}',
        's3.secret-access-key' = '{os.getenv('AWS_SECRET_ACCESS_KEY')}',
        'client.assume-role.region' = '{os.getenv('AWS_REGION')}'
      )
      """
    )


    self.t_env.execute_sql(f"USE CATALOG {self.catalog}")
    self.t_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {self.schema}")

    # Create Iceberg sink table
    self.t_env.execute_sql(f"""
      CREATE TABLE IF NOT EXISTS {self.schema}.{self.table} (
        `timestamp` TIMESTAMP(3),
        `symbol` STRING,
        `action` STRING,
        `amount_usd` FLOAT,
        `amount_btc` FLOAT,
        `balance_usd` FLOAT,
        `balance_btc` FLOAT,
        `price` FLOAT
      ) WITH (
        'format-version'='2',
        'write.format.default'='parquet',
        'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read'
      )
      """
    )

    # Register Kafka source table
    self.t_env.execute_sql(f"""
      CREATE TEMPORARY TABLE kafka_source_{self.topic} (
        `timestamp` TIMESTAMP(3),
        `symbol` STRING,
        `action` STRING,
        `amount_usd` FLOAT,
        `amount_btc` FLOAT,
        `balance_usd` FLOAT,
        `balance_btc` FLOAT,
        `price` FLOAT
      ) WITH (
        'connector' = 'kafka',
        'topic' = '{self.topic}',
        'properties.bootstrap.servers' = '{os.getenv('KAFKA_HOST')}:{os.getenv('KAFKA_PORT')}',
        'format' = 'json',
        'properties.group.id' = 'flink',
        --'json.ignore-parse-errors' = 'true',
        'scan.startup.mode' = 'latest-offset',
        'sink.delivery-guarantee' = 'exactly-once',
        'json.fail-on-missing-field' = 'false'
      )
      """
    )

  def consume_stream(self):
    # Stream from Kafka to Iceberg
    self.t_env.execute_sql(f"""
      INSERT INTO {self.schema}.{self.table}
        SELECT * FROM kafka_source_{self.topic}
    """)
