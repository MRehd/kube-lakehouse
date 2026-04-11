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
        # Catalog is pre-registered via flinkConfiguration (table.catalog.<name>.*)
        # by the Flink Kubernetes Operator — no CREATE CATALOG needed.
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
