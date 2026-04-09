from utils.transactions_loader import FlinkTransactionsLoader as ftl

trans = ftl('bronze', 'finance', 'transactions', 'transactions')

trans.init_entity()
trans.consume_stream()