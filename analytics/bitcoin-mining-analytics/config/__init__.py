# config/__init__.py
"""
Configuration Package for Bitcoin Mining Analytics

Centralized configuration management for the entire platform.
"""

from .constants import *
from .database import db_config, DatabaseConfig
from .api_config import APIConfig
from .logging_config import LoggingConfig

__all__ = [
    'db_config',
    'DatabaseConfig',
    'APIConfig',
    'LoggingConfig'
]

__version__ = '1.0.0'
