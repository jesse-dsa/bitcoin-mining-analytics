# src/models/__init__.py
"""
Models Module for Bitcoin Mining Analytics

This module contains predictive models and optimizers:
- Price and difficulty predictors
- Mining operation optimizers
- Simulation models
"""

from . import predictors
from . import optimizers
from . import simulators

__all__ = [
    'predictors',
    'optimizers',
    'simulators'
]

__version__ = '1.0.0'
