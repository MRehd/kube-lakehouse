from utils.crypto_loader import FlinkCryptoLoader as fcl

btc = fcl('bronze', 'crypto', 'btc', 'btc')

btc.init_entity()
btc.consume_stream()