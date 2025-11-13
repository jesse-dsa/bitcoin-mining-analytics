# src/__init__.py
"""
Bitcoin Mining Analytics - Core Source Code

Main package containing all the source code for the Bitcoin Mining Analytics platform.
"""

from . import data
from . import analytics
from . import models
from . import visualization

__all__ = [
    'data',
    'analytics',
    'models',
    'visualization'
]

__version__ = '1.0.0'

# Package metadata
__author__ = 'Bitcoin Mining Analytics Team'
__description__ = 'Comprehensive analytics platform for Bitcoin mining operations'
__url__ = 'https://github.com/your-username/bitcoin-mining-analytics'
