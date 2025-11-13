# __init__.py
# src/data/collectors/__init__.py
"""
Data Collectors Module for Bitcoin Mining Analytics

This module contains collectors for various data sources:
- Blockchain data (Blockchair, Mempool.space, etc.)
- Mining pool data
- Energy and market data
"""

from .blockchain_collector import BlockchainCollector, collect_blockchain_data

__all__ = [
    'BlockchainCollector',
    'collect_blockchain_data'
]

__version__ = '1.0.0'

# Module metadata
__author__ = 'Bitcoin Mining Analytics Team'
__description__ = 'Data collection utilities for Bitcoin mining analytics'
