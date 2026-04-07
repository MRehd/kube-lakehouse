from utils.transactions_loader import FlinkTransactionsLoader as ftl

trans = ftl('lakehouse', 'bronze', 'transactions', 'transactions')

trans.init_entity()
trans.consume_stream()