import os
import sys

def create_bitcoin_mining_analytics_hybrid():
    """
    Cria a estrutura híbrida completa do repositório Bitcoin Mining Analytics
    Combinando a pipeline de dados com a estrutura de ciência de dados
    """

    # Estrutura híbrida completa
    structure = {
        'data': {
            'raw': {
                'blockchain': [],
                'mining_pools': [],
                'energy': [],
                'hardware': []
            },
            'processed': {
                'network_metrics': [],
                'mining_metrics': [],
                'profitability': []
            },
            'external': {
                'electricity_prices': [],
                'regulatory': [],
                'climate': []
            },
            'real_time': {
                'hash_rate': [],
                'mempool': [],
                'difficulty': []
            },
            'backups': [],
            'exports': []
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
                    'energy_data_collector.py',
                    '__init__.py'
                ],
                'processors': [
                    'data_cleaner.py',
                    'feature_engineer.py',
                    'mining_calculator.py',
                    '__init__.py'
                ],
                'validators': [
                    'data_validator.py',
                    'schema_enforcer.py',
                    '__init__.py'
                ],
                '__init__.py': ''
            },
            'analytics': {
                'mining_economics': [
                    'profitability_calculator.py',
                    'roi_analyzer.py',
                    'break_even_analyzer.py',
                    '__init__.py'
                ],
                'network_analysis': [
                    'hash_rate_analyzer.py',
                    'difficulty_analyzer.py',
                    'block_propagation_analyzer.py',
                    '__init__.py'
                ],
                'sustainability': [
                    'carbon_footprint_calculator.py',
                    'energy_mix_analyzer.py',
                    'efficiency_benchmark.py',
                    '__init__.py'
                ],
                '__init__.py': ''
            },
            'models': {
                'predictors': [
                    'price_predictor.py',
                    'difficulty_predictor.py',
                    'energy_consumption_predictor.py',
                    '__init__.py'
                ],
                'optimizers': [
                    'mining_schedule_optimizer.py',
                    'hardware_allocation_optimizer.py',
                    'energy_cost_optimizer.py',
                    '__init__.py'
                ],
                'simulators': [
                    'mining_farm_simulator.py',
                    'network_growth_simulator.py',
                    'economic_model_simulator.py',
                    '__init__.py'
                ],
                '__init__.py': ''
            },
            'visualization': {
                'dashboards': [
                    'mining_operations_dashboard.py',
                    'profitability_dashboard.py',
                    'sustainability_dashboard.py',
                    '__init__.py'
                ],
                'reports': [
                    'mining_report_generator.py',
                    'performance_reporter.py',
                    'alert_system.py',
                    '__init__.py'
                ],
                '__init__.py': ''
            },
            '__init__.py': ''
        },

        'scripts': [
            'bitcoin_blockchair_dashboard.py',
            'scheduled_collector.py',
            'real_time_monitor.py',
            'data_pipeline.py'
        ],

        'database': [
            'postgres_manager.py',
            'duckdb_manager.py',
            'base_manager.py',
            '__init__.py'
        ],

        'config': [
            'data_sources.yaml',
            'mining_parameters.yaml',
            'model_configs.yaml',
            'constants.py',
            'database.py',
            'api_config.py',
            'logging_config.py',
            '__init__.py'
        ],

        'utils': [
            'hash_calculator.py',
            'data_processor.py',
            'formatters.py',
            '__init__.py'
        ],

        'analysis': [
            'price_correlation.py',
            'mining_profitability.py',
            'network_health.py',
            '__init__.py'
        ],

        'infrastructure': {
            'docker': [
                'Dockerfile',
                'docker-compose.yml',
                'requirements.txt'
            ],
            'airflow': {
                'dags': [
                    'bitcoin_data_pipeline.py',
                    'mining_metrics_dag.py'
                ],
                'config': [
                    'airflow.cfg'
                ]
            },
            'monitoring': [
                'prometheus.yml',
                'grafana_dashboard.json'
            ]
        },

        'results': {
            'model_performance': [],
            'financial_analysis': [],
            'sustainability_reports': [],
            'research_papers': []
        },

        'docs': {
            'methodology_papers': [],
            'api_endpoints.md': '',
            'setup_guide.md': '',
            'database_schema.md': ''
        },

        'tests': [
            'test_data_collectors.py',
            'test_analytics.py',
            'test_models.py',
            '__init__.py'
        ],

        'logs': []
    }

    def create_structure(base_path, structure, current_path=""):
        """Cria recursivamente a estrutura de diretórios e arquivos"""
        for name, content in structure.items():
            path = os.path.join(base_path, current_path, name)

            if isinstance(content, dict):
                # É um diretório
                os.makedirs(path, exist_ok=True)
                print(f"📁 Criado: {path}")

                # Criar __init__.py se for módulo Python
                if name in ['src', 'data', 'analytics', 'models', 'visualization', 'collectors', 'processors', 'validators', 'mining_economics', 'network_analysis', 'sustainability', 'predictors', 'optimizers', 'simulators', 'dashboards', 'reports']:
                    init_file = os.path.join(path, '__init__.py')
                    with open(init_file, 'w') as f:
                        f.write(f'# {name} module\n')
                    print(f"  📄 {init_file}")

                # Recursão para subdiretórios
                create_structure(base_path, content, os.path.join(current_path, name))

            elif isinstance(content, list):
                # É uma lista de arquivos
                os.makedirs(path, exist_ok=True)
                print(f"📁 Criado: {path}")

                for item in content:
                    if '.' in item:  # É um arquivo
                        file_path = os.path.join(path, item)
                        with open(file_path, 'w') as f:
                            f.write(f'# {item}\n')
                        print(f"  📄 {file_path}")
                    else:  # É um diretório vazio
                        dir_path = os.path.join(path, item)
                        os.makedirs(dir_path, exist_ok=True)
                        print(f"  📁 {dir_path}")
            else:
                # É um arquivo (string) em dict como valor
                file_path = os.path.join(base_path, current_path, name)
                with open(file_path, 'w') as f:
                    f.write(f'# {name}\n')
                print(f"📄 Criado: {file_path}")

    # Criar arquivos raiz
    root_files = {
        'requirements.txt': '# Project dependencies\n',
        '.env.example': '# Environment variables template\n',
        '.gitignore': '# Git ignore rules\n',
        'README.md': '# Bitcoin Mining Analytics\n',
        'docker-compose.yml': '# Docker composition\n',
        'setup.py': '# Project setup\n'
    }

    # Diretório base
    base_path = "bitcoin-mining-analytics"

    print("🚀 CRIANDO ESTRUTURA HÍBRIDA BITCOIN MINING ANALYTICS")
    print("=" * 60)

    # Criar diretório principal
    if os.path.exists(base_path):
        print(f"⚠️  Diretório {base_path} já existe. Continuando...")
    else:
        os.makedirs(base_path, exist_ok=True)
        print(f"📁 Criado: {base_path}")

    # Criar estrutura principal
    create_structure(base_path, structure)

    # Criar arquivos raiz
    for filename, content in root_files.items():
        file_path = os.path.join(base_path, filename)
        with open(file_path, 'w') as f:
            f.write(content)
        print(f"📄 Criado: {file_path}")

    print("=" * 60)
    print(f"✅ ESTRUTURA CRIADA COM SUCESSO!")
    print(f"📍 Local: {os.path.abspath(base_path)}")

    # Mostrar resumo da estrutura
    print("\n📋 RESUMO DA ESTRUTURA:")
    print_summary(base_path)

def count_files(path):
    """Conta o número total de arquivos na estrutura"""
    count = 0
    for root, dirs, files in os.walk(path):
        count += len(files)
    return count

def print_summary(base_path):
    """Imprime resumo da estrutura criada"""
    summary = {
        '📁 Diretórios de Dados': ['data/raw', 'data/processed', 'data/external', 'data/real_time'],
        '📓 Notebooks de Análise': ['notebooks/01_data_collection', 'notebooks/02_eda', 'notebooks/03_feature_engineering', 'notebooks/04_modeling'],
        '🔧 Módulos Source': ['src/data', 'src/analytics', 'src/models', 'src/visualization'],
        '⚡ Scripts Principais': ['scripts'],
        '🗄️  Banco de Dados': ['database'],
        '⚙️  Configurações': ['config'],
        '🏗️  Infraestrutura': ['infrastructure/docker', 'infrastructure/airflow', 'infrastructure/monitoring'],
        '📈 Resultados': ['results/model_performance', 'results/financial_analysis', 'results/sustainability_reports'],
        '📚 Documentação': ['docs'],
        '🧪 Testes': ['tests']
    }

    for category, paths in summary.items():
        print(f"\n{category}:")
        for path in paths:
            full_path = os.path.join(base_path, path)
            if os.path.exists(full_path):
                file_count = len([f for f in os.listdir(full_path) if os.path.isfile(os.path.join(full_path, f))])
                print(f"  └── {path}/ ({file_count} arquivos)")

    total_files = count_files(base_path)
    print(f"\n📊 TOTAL: {total_files} arquivos criados")

if __name__ == "__main__":
    create_bitcoin_mining_analytics_hybrid()
