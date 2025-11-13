# analysis/__init__.py
"""
Analysis Module for Bitcoin Mining Analytics

This module contains specialized analysis scripts:
- Price correlation analysis
- Mining profitability analysis
- Network health analysis
- Advanced analytics
"""

from .price_correlation import PriceCorrelationAnalyzer
from .mining_profitability import MiningProfitabilityAnalyzer
from .network_health import NetworkHealthAnalyzer

__all__ = [
    'PriceCorrelationAnalyzer',
    'MiningProfitabilityAnalyzer',
    'NetworkHealthAnalyzer'
]

__version__ = '1.0.0'
