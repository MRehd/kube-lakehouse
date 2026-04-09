from utils.crypto_loader import FlinkCryptoLoader as fcl

eth = fcl('bronze', 'crypto', 'eth', 'eth')

eth.init_entity()
eth.consume_stream()