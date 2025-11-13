# src/data/__init__.py
"""
Data Module for Bitcoin Mining Analytics

This module handles all data-related operations:
- Data collection from various sources
- Data processing and transformation
- Data validation and quality control
"""

from . import collectors
from . import processors
from . import validators

__all__ = [
    'collectors',
    'processors',
    'validators'
]

__version__ = '1.0.0'
