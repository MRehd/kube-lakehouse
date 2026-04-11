import os
from pyflink.table import StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment


class FlinkTransactionsLoader:

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
                'scope'               = 'PRINCIPAL_ROLE:ALL',
                's3.endpoint'         = '{os.getenv("S3_ENDPOINT")}',
                's3.access-key'       = '{os.getenv("S3_ACCESS_KEY")}',
                's3.secret-key'       = '{os.getenv("S3_SECRET_KEY")}',
                's3.path-style-access'= 'true',
                's3.region'           = '{os.getenv("S3_REGION")}'
            )
        """)
        self.t_env.execute_sql(f'USE CATALOG {self.catalog}')
        self.t_env.execute_sql(f'CREATE DATABASE IF NOT EXISTS {self.schema}')

        self.t_env.execute_sql(f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{self.table} (
                `timestamp`   TIMESTAMP(3),
                `symbol`      STRING,
                `action`      STRING,
                `amount_usd`  FLOAT,
                `amount_btc`  FLOAT,
                `balance_usd` FLOAT,
                `balance_btc` FLOAT,
                `price`       FLOAT
            ) WITH (
                'format-version'='2',
                'write.format.default'='parquet',
                'write.delete.mode'='merge-on-read',
                'write.update.mode'='merge-on-read'
            )
        """)

        self.t_env.execute_sql(f"""
            CREATE TEMPORARY TABLE kafka_source_{self.topic} (
                `timestamp`   TIMESTAMP(3),
                `symbol`      STRING,
                `action`      STRING,
                `amount_usd`  FLOAT,
                `amount_btc`  FLOAT,
                `balance_usd` FLOAT,
                `balance_btc` FLOAT,
                `price`       FLOAT
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