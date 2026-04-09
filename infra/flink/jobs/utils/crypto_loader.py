import os
from pyflink.table import StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment


class FlinkCryptoLoader:

    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    def __init__(self, catalog, schema, table, topic):
        self.catalog = catalog
        self.schema = schema
        self.table = table
        self.topic = topic

    def init_entity(self):
        client_id     = os.getenv(f'POLARIS_{self.catalog.upper()}_CLIENT_ID', '')
        client_secret = os.getenv(f'POLARIS_{self.catalog.upper()}_CLIENT_SECRET', '')

        self.t_env.execute_sql(f"""
            CREATE CATALOG IF NOT EXISTS {self.catalog} WITH (
                'type'                = 'iceberg',
                'catalog-type'        = 'rest',
                'uri'                 = '{os.getenv("POLARIS_ENDPOINT")}/api/catalog',
                'warehouse'           = '{self.catalog}',
                'credential'          = '{client_id}:{client_secret}',
                's3.endpoint'         = '{os.getenv("S3_ENDPOINT")}',
                's3.access-key'       = '{os.getenv("S3_ACCESS_KEY")}',
                's3.secret-key'       = '{os.getenv("S3_SECRET_KEY")}',
                's3.path-style-access'= 'true'
            )
        """)
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
