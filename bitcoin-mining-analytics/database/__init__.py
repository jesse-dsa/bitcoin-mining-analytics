# database/__init__.py
"""
Database Module for Bitcoin Mining Analytics

This module contains database management utilities:
- PostgreSQL integration
- DuckDB local storage
- Data persistence and retrieval
- Backup management
"""

try:
    from .duckdb_manager import DuckDBManager, get_db_manager
    __all__ = [
        'DuckDBManager',
        'get_db_manager'
    ]
except ImportError as e:
    print(f"⚠️ DuckDBManager não disponível: {e}")
    __all__ = []

__version__ = '1.0.0'
