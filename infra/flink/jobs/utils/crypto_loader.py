import os
from pyflink.table import StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment


class FlinkCryptoLoader:

    jars = [
        'file:///opt/flink/lib/flink-connector-kafka-3.4.0-1.20.jar',
        'file:///opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar',
        'file:///opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar',
        'file:///opt/flink/lib/iceberg-flink-runtime-1.20-1.8.1.jar',
    ]

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)
    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set('pipeline.jars', ';'.join(jars))
    t_env.get_config().set('pipeline.classpaths', ';'.join(jars))
    t_env.get_config().set('classloader.resolve-order', 'parent-first')
    t_env.get_config().set('classloader.parent-first-patterns.additional', 'com.codahale.metrics')

    def __init__(self, catalog, schema, table, topic):
        self.catalog = catalog
        self.schema = schema
        self.table = table
        self.topic = topic

    def init_entity(self):
        # Catalog is pre-registered by the Flink operator via flinkConfiguration
        self.t_env.execute_sql(f'USE CATALOG {self.catalog}')
        self.t_env.execute_sql(f'CREATE DATABASE IF NOT EXISTS {self.schema}')

        self.t_env.execute_sql(f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{self.table} (
                `Timestamp` TIMESTAMP(3),
                `Low`       FLOAT,
                `High`      FLOAT,
                `Open`      FLOAT,
                `Close`     FLOAT,
                `Volume`    FLOAT
            ) WITH (
                'format-version'='2',
                'write.format.default'='parquet',
                'write.delete.mode'='merge-on-read',
                'write.update.mode'='merge-on-read',
                'history.expire.max-snapshot-age-ms'='3600000',
                'history.expire.min-snapshots-to-keep'='1'
            )
        """)

        self.t_env.execute_sql(f"""
            CREATE TEMPORARY TABLE kafka_source_{self.topic} (
                `Timestamp` TIMESTAMP(3),
                `Low`       FLOAT,
                `High`      FLOAT,
                `Open`      FLOAT,
                `Close`     FLOAT,
                `Volume`    FLOAT
            ) WITH (
                'connector'                    = 'kafka',
                'topic'                        = '{self.topic}',
                'properties.bootstrap.servers' = '{os.getenv("KAFKA_BOOTSTRAP_SERVERS")}',
                'format'                       = 'json',
                'properties.group.id'          = 'flink',
                'scan.startup.mode'            = 'latest-offset',
                'sink.delivery-guarantee'      = 'exactly-once',
                'json.fail-on-missing-field'   = 'false'
            )
        """)

    def consume_stream(self):
        self.t_env.execute_sql(f"""
            INSERT INTO {self.schema}.{self.table}
            SELECT * FROM kafka_source_{self.topic}
        """)
