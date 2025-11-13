# config/database.py
"""
Configurações de banco de dados para Bitcoin Mining Analytics
"""
import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class DatabaseConfig:
    """Configurações para conexões com banco de dados"""

    # PostgreSQL Configuration
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "bitcoin_mining_analytics")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "")

    @property
    def POSTGRES_URL(self) -> str:
        """Retorna a URL de conexão PostgreSQL"""
        if self.POSTGRES_PASSWORD:
            return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        else:
            return f"postgresql://{self.POSTGRES_USER}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    # DuckDB Configuration
    DUCKDB_PATH: str = os.getenv("DUCKDB_PATH", "data/bitcoin_analytics.duckdb")

    # Backup Configuration
    BACKUP_DIR: str = "data/backups"
    BACKUP_RETENTION_DAYS: int = 30

    # Table Names
    METRICS_TABLE: str = "bitcoin_network_metrics"
    MINING_METRICS_TABLE: str = "mining_operation_metrics"
    SNAPSHOTS_TABLE: str = "bitcoin_snapshots"

    # Connection Pool Settings
    MAX_CONNECTIONS: int = 10
    CONNECTION_TIMEOUT: int = 30

# Instância global de configuração
db_config = DatabaseConfig()
