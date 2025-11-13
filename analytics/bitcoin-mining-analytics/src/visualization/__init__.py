# src/visualization/__init__.py
"""
Visualization Module for Bitcoin Mining Analytics

This module contains visualization utilities:
- Interactive dashboards
- Report generation
- Alert systems
"""

from . import dashboards
from . import reports

__all__ = [
    'dashboards',
    'reports'
]

__version__ = '1.0.0'
