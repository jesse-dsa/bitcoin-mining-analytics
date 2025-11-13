# src/analytics/__init__.py
"""
Analytics Module for Bitcoin Mining Analytics

This module contains analytical capabilities:
- Mining economics and profitability analysis
- Network analysis and monitoring
- Sustainability and efficiency analytics
"""

from . import mining_economics
from . import network_analysis
from . import sustainability

__all__ = [
    'mining_economics',
    'network_analysis',
    'sustainability'
]

__version__ = '1.0.0'
