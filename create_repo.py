import os
import yaml

def create_bitcoin_mining_analytics_repo():
    """
    Cria a estrutura completa do repositório de Ciência de Dados
    para Mineração de Bitcoin com arquivos iniciais
    """

    # Estrutura de diretórios
    structure = {
        'data': {
            'raw': ['blockchain_raw', 'mining_pools', 'energy_data', 'hardware_specs'],
            'processed': ['mining_metrics', 'profitability', 'network_stats'],
            'external': ['electricity_prices', 'regulatory_data', 'climate_data'],
            'real_time': ['hash_rate_stream', 'mempool_data', 'difficulty_adjustments']
        },
        'notebooks': {
            '01_data_collection': [
                'blockchain_data_extraction.ipynb',
                'mining_pool_analysis.ipynb',
                'energy_consumption_calculation.ipynb'
            ],
            '02_eda': [
                'mining_profitability_analysis.ipynb',
                'hash_rate_distribution.ipynb',
                'hardware_efficiency_trends.ipynb'
            ],
            '03_feature_engineering': [
                'mining_difficulty_features.ipynb',
                'energy_cost_features.ipynb',
                'regulatory_impact_features.ipynb'
            ],
            '04_modeling': [
                'profitability_prediction.ipynb',
                'hash_rate_forecasting.ipynb',
                'mining_operation_optimization.ipynb'
            ]
        },
        'src': {
            'data': {
                'collectors': [
                    'blockchain_collector.py',
                    'mining_pool_collector.py',
                    'energy_data_collector.py'
                ],
                'processors': [
                    'data_cleaner.py',
                    'feature_engineer.py',
                    'mining_calculator.py'
                ],
                'validators': [
                    'data_validator.py',
                    'schema_enforcer.py'
                ]
            },
            'analytics': {
                'mining_economics': [
                    'profitability_calculator.py',
                    'roi_analyzer.py',
                    'break_even_analyzer.py'
                ],
                'network_analysis': [
                    'hash_rate_analyzer.py',
                    'difficulty_analyzer.py',
                    'block_propagation_analyzer.py'
                ],
                'sustainability': [
                    'carbon_footprint_calculator.py',
                    'energy_mix_analyzer.py',
                    'efficiency_benchmark.py'
                ]
            },
            'models': {
                'predictors': [
                    'price_predictor.py',
                    'difficulty_predictor.py',
                    'energy_consumption_predictor.py'
                ],
                'optimizers': [
                    'mining_schedule_optimizer.py',
                    'hardware_allocation_optimizer.py',
                    'energy_cost_optimizer.py'
                ],
                'simulators': [
                    'mining_farm_simulator.py',
                    'network_growth_simulator.py',
                    'economic_model_simulator.py'
                ]
            },
            'visualization': {
                'dashboards': [
                    'mining_operations_dashboard.py',
                    'profitability_dashboard.py',
                    'sustainability_dashboard.py'
                ],
                'reports': [
                    'mining_report_generator.py',
                    'performance_reporter.py',
                    'alert_system.py'
                ]
            }
        },
        'config': [],
        'tests': [],
        'docs': ['methodology_papers'],
        'scripts': [],
        'infrastructure': {
            'docker': [],
            'airflow': ['dags', 'config'],
            'monitoring': []
        },
        'results': [
            'model_performance',
            'financial_analysis',
            'sustainability_reports',
            'research_papers'
        ],
        'logs': []
    }

    # Criar estrutura de diretórios
    base_path = "bitcoin-mining-analytics"

    def create_dirs(base, structure):
        for item, content in structure.items():
            path = os.path.join(base, item)
            os.makedirs(path, exist_ok=True)

            # Criar arquivo __init__.py para módulos Python
            if item in ['src', 'data', 'analytics', 'models', 'visualization']:
                init_file = os.path.join(path, '__init__.py')
                with open(init_file, 'w') as f:
                    f.write('# Bitcoin Mining Analytics Module\n')

            if isinstance(content, dict):
                create_dirs(path, content)
            elif isinstance(content, list):
                for subitem in content:
                    if '.' in subitem:  # É um arquivo
                        file_path = os.path.join(path, subitem)
                        with open(file_path, 'w') as f:
                            f.write(f'# {subitem}\n\n')
                    else:  # É um diretório
                        dir_path = os.path.join(path, subitem)
                        os.makedirs(dir_path, exist_ok=True)

    # Criar diretórios principais
    create_dirs('.', {base_path: structure})

    # Criar arquivos de configuração
    config_files(base_path)

    # Criar arquivos de documentação
    create_documentation(base_path)

    # Criar scripts principais
    create_main_scripts(base_path)

    print(f"✅ Repositório criado com sucesso em: {base_path}/")
    print("📁 Estrutura completa gerada com arquivos iniciais")

def config_files(base_path):
    """Cria arquivos de configuração YAML"""

    # config/data_sources.yaml
    data_sources = {
        'blockchain_data': {
            'blockchain_com': 'https://api.blockchain.com/v3',
            'blockchair': 'https://api.blockchair.com/bitcoin',
            'mempool_space': 'https://mempool.space/api'
        },
        'mining_data': {
            'blockchain_com_mining': 'https://api.blockchain.com/mining',
            'btc_com': 'https://pool.api.btc.com/v1',
            'slushpool': 'https://slushpool.com/api'
        },
        'energy_data': {
            'eia_gov': 'https://api.eia.gov/v2',
            'electricity_maps': 'https://api.electricitymap.org'
        },
        'market_data': {
            'coinmetrics': 'https://api.coinmetrics.io/v4',
            'glassnode': 'https://api.glassnode.com/v1',
            'cryptocompare': 'https://min-api.cryptocompare.com/data'
        }
    }

    with open(f'{base_path}/config/data_sources.yaml', 'w') as f:
        yaml.dump(data_sources, f, default_flow_style=False)

    # config/mining_parameters.yaml
    mining_params = {
        'asic_models': {
            'antminer_s19_xp': {
                'hash_rate': 140,
                'power_consumption': 3010,
                'efficiency': 21.5,
                'cost': 4500,
                'lifespan_months': 24
            },
            'antminer_s19j_pro': {
                'hash_rate': 104,
                'power_consumption': 3068,
                'efficiency': 29.5,
                'cost': 3200,
                'lifespan_months': 24
            },
            'whatsminer_m50': {
                'hash_rate': 118,
                'power_consumption': 3276,
                'efficiency': 27.8,
                'cost': 3800,
                'lifespan_months': 24
            }
        },
        'energy_sources': {
            'industrial_rate': 0.08,
            'renewable_rate': 0.12,
            'offpeak_discount': 0.3,
            'cooling_cost_multiplier': 1.15
        },
        'network_parameters': {
            'block_reward': 6.25,
            'block_time_target': 600,
            'difficulty_adjustment_blocks': 2016,
            'halving_blocks': 210000
        }
    }

    with open(f'{base_path}/config/mining_parameters.yaml', 'w') as f:
        yaml.dump(mining_params, f, default_flow_style=False)

    # config/model_configs.yaml
    model_configs = {
        'profitability_prediction': {
            'model': 'xgboost',
            'features': [
                'hash_rate_7d_avg',
                'difficulty_30d_change',
                'btc_price_30d_volatility',
                'energy_cost_region',
                'hardware_efficiency'
            ],
            'hyperparameters': {
                'n_estimators': 100,
                'max_depth': 6,
                'learning_rate': 0.1
            }
        },
        'hash_rate_forecasting': {
            'model': 'prophet',
            'seasonality': {
                'yearly': True,
                'weekly': True,
                'daily': False
            },
            'changepoint_prior_scale': 0.05
        }
    }

    with open(f'{base_path}/config/model_configs.yaml', 'w') as f:
        yaml.dump(model_configs, f, default_flow_style=False)

    # config/constants.py
    constants_content = '''
# Bitcoin Mining Analytics - Constants

# Network Constants
BLOCK_REWARD = 6.25
HALVING_BLOCKS = 210000
DIFFICULTY_ADJUSTMENT_BLOCKS = 2016
BLOCK_TIME_TARGET = 600  # seconds

# Energy Constants
JOULES_PER_KWH = 3.6e6
CARBON_INTENSITY_AVG = 0.475  # kgCO2/kWh global average

# Economic Constants
DAYS_PER_MONTH = 30.44
MONTHS_PER_YEAR = 12

# API Constants
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RATE_LIMIT_DELAY = 1.0

# Mining Pool Constants
MAJOR_POOLS = [
    "Foundry USA", "AntPool", "F2Pool", "Binance Pool",
    "ViaBTC", "Poolin", "BTC.com", "SlushPool"
]
'''

    with open(f'{base_path}/config/constants.py', 'w') as f:
        f.write(constants_content)

def create_documentation(base_path):
    """Cria arquivos de documentação"""

    # README.md principal
    readme_content = ''

if __name__ == "__main__":
    create_bitcoin_mining_analytics_repo()
