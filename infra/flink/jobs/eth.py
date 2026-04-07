from utils.crypto_loader import FlinkCryptoLoader as fcl

eth = fcl('lakehouse', 'bronze', 'eth', 'eth')

eth.init_entity()
eth.consume_stream()