# config/api_config.py
"""
Configurações de APIs externas para coleta de dados Bitcoin
"""

# Blockchair API Configuration
BLOCKCHAIR_CONFIG = {
    'base_url': 'https://api.blockchair.com',
    'endpoints': {
        'bitcoin_stats': '/bitcoin/stats',
        'blocks': '/bitcoin/blocks',
        'transactions': '/bitcoin/transactions',
        'mempool': '/bitcoin/mempool'
    },
    'rate_limit': 1000,  # requests per hour
    'timeout': 30
}

# Blockchain.com API
BLOCKCHAIN_CONFIG = {
    'base_url': 'https://blockchain.info',
    'endpoints': {
        'ticker': '/ticker',
        'charts': '/charts',
        'pool_stats': '/pools'
    }
}

# CoinGecko API (para preços e market data)
COINGECKO_CONFIG = {
    'base_url': 'https://api.coingecko.com/api/v3',
    'endpoints': {
        'bitcoin': '/coins/bitcoin',
        'market_chart': '/coins/bitcoin/market_chart'
    }
}

# Mining Pool APIs
MINING_POOLS_CONFIG = {
    'antpool': 'https://www.antpool.com/api',
    'f2pool': 'https://api.f2pool.com',
    'poolin': 'https://www.poolin.com/api'
}

# Energy Data APIs
ENERGY_APIS = {
    'eia': 'https://api.eia.gov/v2',
    'energy_price': 'https://api.energypriceapi.com/v1'
}
