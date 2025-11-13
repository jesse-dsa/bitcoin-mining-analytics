# utils/__init__.py
"""
Utilities Module for Bitcoin Mining Analytics

This module contains general utility functions:
- Hash rate calculations
- Data processing helpers
- Formatting utilities
- Common helpers
"""

from .hash_calculator import HashRateCalculator
from .data_processor import DataProcessor
from .formatters import format_currency, format_hashrate, format_percentage

__all__ = [
    'HashRateCalculator',
    'DataProcessor',
    'format_currency',
    'format_hashrate',
    'format_percentage'
]

__version__ = '1.0.0'
