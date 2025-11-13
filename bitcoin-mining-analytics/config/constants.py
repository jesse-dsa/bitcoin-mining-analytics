# config/constants.py
"""
Bitcoin Mining Analytics - Constants
Constantes globais para o projeto de análise de mineração Bitcoin
"""

# =============================================================================
# NETWORK CONSTANTS - Constantes da Rede Bitcoin
# =============================================================================
BLOCK_REWARD = 6.25  # Recompensa atual por bloco (BTC)
HALVING_BLOCKS = 210000  # Blocos entre cada halving
DIFFICULTY_ADJUSTMENT_BLOCKS = 2016  # Blocos entre ajustes de dificuldade
BLOCK_TIME_TARGET = 600  # Tempo alvo por bloco em segundos (10 minutos)
SATOSHIS_PER_BTC = 100000000  # Satoshis em 1 BTC

# =============================================================================
# ENERGY CONSTANTS - Constantes de Energia
# =============================================================================
JOULES_PER_KWH = 3.6e6  # Joules por kWh
WATTS_PER_TERAHASH = 30  # Watts por TH/s (estimativa conservadora)
CARBON_INTENSITY_AVG = 0.475  # kgCO2/kWh (média global)
COOLING_COST_MULTIPLIER = 1.15  # Multiplicador para custos de refrigeração

# =============================================================================
# ECONOMIC CONSTANTS - Constantes Econômicas
# =============================================================================
DAYS_PER_MONTH = 30.44
MONTHS_PER_YEAR = 12
HOURS_PER_DAY = 24
SECONDS_PER_HOUR = 3600
SECONDS_PER_DAY = 86400

# =============================================================================
# API CONSTANTS - Constantes de API
# =============================================================================
REQUEST_TIMEOUT = 30  # Timeout para requisições em segundos
MAX_RETRIES = 3  # Máximo de tentativas de requisição
RATE_LIMIT_DELAY = 1.0  # Delay entre requisições em segundos
DEFAULT_USER_AGENT = "Bitcoin-Mining-Analytics/1.0"

# =============================================================================
# MINING POOL CONSTANTS - Constantes de Mining Pools
# =============================================================================
MAJOR_POOLS = [
    "Foundry USA",
    "AntPool",
    "F2Pool",
    "Binance Pool",
    "ViaBTC",
    "Poolin",
    "BTC.com",
    "SlushPool"
]

# =============================================================================
# HARDWARE CONSTANTS - Constantes de Hardware
# =============================================================================
DEFAULT_HASH_RATE_UNIT = "TH/s"  # Unidade padrão para hash rate
DEFAULT_POWER_UNIT = "W"  # Unidade padrão para consumo energético
DEFAULT_EFFICIENCY_UNIT = "J/TH"  # Unidade padrão para eficiência

# =============================================================================
# DATA COLLECTION CONSTANTS - Constantes de Coleta de Dados
# =============================================================================
UPDATE_INTERVAL_BASIC = 300  # 5 minutos para dados básicos
UPDATE_INTERVAL_DETAILED = 3600  # 1 hora para dados detalhados
DATA_RETENTION_DAYS = 365  # Manter dados por 1 ano

# =============================================================================
# FILE PATHS - Caminhos de Arquivos
# =============================================================================
DATA_RAW_PATH = "data/raw"
DATA_PROCESSED_PATH = "data/processed"
DATA_BACKUPS_PATH = "data/backups"
LOGS_PATH = "logs"
RESULTS_PATH = "results"

# =============================================================================
# LOGGING CONSTANTS - Constantes de Log
# =============================================================================
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
