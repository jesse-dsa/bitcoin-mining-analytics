# scripts/bitcoin_blockchair_dashboard.py
import asyncio
import aiohttp
import json
import sys
import os
from datetime import datetime
import logging

# Adicionar diretório pai ao path para imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Tentar importar o coletor modular
try:
    from src.data.collectors.blockchain_collector import get_primary_metrics, collect_blockchain_data
    COLLECTOR_AVAILABLE = True
    print("✅ Coletor modular carregado com sucesso")
except ImportError as e:
    print(f"⚠️ Coletor modular não disponível: {e}")
    COLLECTOR_AVAILABLE = False

# Tentar importar o gerenciador de banco
try:
    from database.duckdb_manager import DuckDBManager
    DB_AVAILABLE = True
    print("✅ Gerenciador de banco DuckDB carregado")
except ImportError as e:
    print(f"⚠️ Gerenciador de banco não disponível: {e}")
    DB_AVAILABLE = False

class BitcoinBlockchairDashboard:
    """
    Dashboard principal para coleta e análise de dados Bitcoin
    Versão com DuckDBManager totalmente integrado
    """

    def __init__(self, use_modular_collector=True, enable_database=True):
        self.base_url = "https://api.blockchair.com/bitcoin/stats"
        self.data = None
        self.use_modular_collector = use_modular_collector and COLLECTOR_AVAILABLE
        self.enable_database = enable_database and DB_AVAILABLE
        self.setup_directories()
        self.setup_logging()

        # ✅ INICIALIZAÇÃO CORRETA DO BANCO
        if self.enable_database:
            try:
                self.db_manager = DuckDBManager()
                self.logger.info("🗄️ Persistência em banco ativada - DuckDBManager inicializado")
            except Exception as e:
                self.logger.error(f"❌ Falha ao inicializar DuckDBManager: {e}")
                self.enable_database = False
                self.db_manager = None
        else:
            self.db_manager = None
            self.logger.info("📄 Persistência em banco desativada")

        if self.use_modular_collector:
            self.logger.info("🚀 Coletor modular ativado")
        else:
            self.logger.info("🔄 Usando coletor direto (fallback)")

    def setup_directories(self):
        """Cria diretórios necessários"""
        directories = [
            'data/raw/blockchain',
            'data/backups',
            'logs',
            'results/financial_analysis',
            'exports'
        ]

        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            print(f"📁 Diretório criado/verificado: {directory}")

    def setup_logging(self):
        """Configura o sistema de logging"""
        log_file = 'logs/dashboard.log'

        # Garantir que o diretório de logs existe
        os.makedirs('logs', exist_ok=True)

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(log_file, encoding='utf-8')
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("✅ Sistema de logging configurado")

    async def fetch_blockchain_data_modular(self):
        """
        Coleta dados usando o coletor modular
        """
        try:
            self.logger.info("🔄 Usando coletor modular para coleta de dados...")
            data = await get_primary_metrics()

            if data and len(data) > 0:
                self.logger.info(f"✅ Coletor modular: {len(data)} campos coletados")
                return data
            else:
                self.logger.warning("⚠️ Coletor modular retornou dados vazios")
                return None

        except Exception as e:
            self.logger.error(f"❌ Erro no coletor modular: {e}")
            return None

    async def fetch_blockchain_data_direct(self):
        """
        Coleta dados diretamente da API Blockchair (fallback)
        """
        async with aiohttp.ClientSession() as session:
            try:
                self.logger.info(f"🌐 Conectando diretamente: {self.base_url}")
                print(f"🌐 Conectando: {self.base_url}")

                async with session.get(self.base_url, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.logger.info("✅ Dados coletados com sucesso via API direta")
                        print("✅ Dados coletados com sucesso")
                        return data['data']
                    else:
                        error_msg = f"❌ Erro na API direta: Status {response.status}"
                        self.logger.error(error_msg)
                        print(error_msg)
                        return None

            except asyncio.TimeoutError:
                error_msg = "⏰ Timeout na requisição direta"
                self.logger.error(error_msg)
                print(error_msg)
                return None
            except Exception as e:
                error_msg = f"❌ Erro na requisição direta: {e}"
                self.logger.error(error_msg)
                print(error_msg)
                return None

    async def fetch_blockchain_data(self):
        """
        Coleta dados - usa coletor modular se disponível, fallback para direto
        """
        if self.use_modular_collector:
            modular_data = await self.fetch_blockchain_data_modular()
            if modular_data:
                return modular_data
            else:
                self.logger.warning("🔁 Coletor modular falhou, tentando método direto...")

        # Fallback para método direto
        return await self.fetch_blockchain_data_direct()

    def process_hashrate(self, hashrate_str):
        """
        Processa e converte o hash rate para diferentes unidades
        """
        try:
            hashrate_hs = int(hashrate_str)

            conversions = {
                'hash/s': hashrate_hs,
                'kilohash/s': hashrate_hs / 1e3,
                'megahash/s': hashrate_hs / 1e6,
                'gigahash/s': hashrate_hs / 1e9,
                'terahash/s': hashrate_hs / 1e12,
                'petahash/s': hashrate_hs / 1e15,
                'exahash/s': hashrate_hs / 1e18
            }

            return conversions

        except (ValueError, TypeError) as e:
            self.logger.error(f"❌ Erro ao processar hash rate: {e}")
            print(f"❌ Erro ao processar hash rate: {e}")
            return {}

    def calculate_mining_metrics(self, data):
        """
        Calcula métricas de mineração baseadas nos dados da rede
        """
        if not data:
            return {}

        try:
            # Hash rate em EH/s para cálculos
            hashrate_ehs = self.process_hashrate(data.get('hashrate_24h', '0')).get('exahash/s', 0)
            difficulty = data.get('difficulty', 0)
            block_reward = 6.25  # BTC
            btc_price = data.get('market_price_usd', 0)

            # Cálculos básicos de mineração
            daily_blocks = data.get('blocks_24h', 144)
            network_hashrate_th = hashrate_ehs * 1e6  # Converter EH/s para TH/s

            # Receita diária da rede (aproximada)
            daily_network_revenue = daily_blocks * block_reward * btc_price

            # Hash price (USD per TH/s per day)
            hash_price = daily_network_revenue / network_hashrate_th if network_hashrate_th > 0 else 0

            metrics = {
                'hashrate_ehs': hashrate_ehs,
                'hashrate_phs': hashrate_ehs * 1000,  # PH/s
                'daily_blocks': daily_blocks,
                'block_time_actual': (24 * 3600) / daily_blocks if daily_blocks > 0 else 600,
                'daily_network_revenue_usd': daily_network_revenue,
                'hash_price_usd_per_th_per_day': hash_price,
                'network_efficiency_j_per_th': 30,  # Estimativa conservadora
                'estimated_daily_energy_consumption_gwh': (hashrate_ehs * 1e6 * 30 * 24) / 1e9,  # GWh corrigido
            }

            return metrics

        except Exception as e:
            self.logger.error(f"❌ Erro ao calcular métricas de mineração: {e}")
            print(f"❌ Erro ao calcular métricas de mineração: {e}")
            return {}

    def analyze_profitability(self, data, mining_metrics):
        """
        Analisa a lucratividade básica da mineração
        """
        if not data or not mining_metrics:
            return {}

        try:
            btc_price = data.get('market_price_usd', 0)
            hash_price = mining_metrics.get('hash_price_usd_per_th_per_day', 0)

            # Parâmetros de um minerador S19 XP (140 TH/s, 3010W)
            miner_hashrate_th = 140
            miner_power_consumption_w = 3010
            energy_cost_per_kwh = 0.08  # USD

            # Cálculos de lucratividade
            daily_revenue = hash_price * miner_hashrate_th
            daily_energy_cost = (miner_power_consumption_w * 24 / 1000) * energy_cost_per_kwh
            daily_profit = daily_revenue - daily_energy_cost

            # ROI básico (considerando custo do hardware de $4500)
            hardware_cost = 4500
            roi_days = hardware_cost / daily_profit if daily_profit > 0 else float('inf')

            profitability = {
                'miner_model': 'Antminer S19 XP',
                'miner_hashrate_th': miner_hashrate_th,
                'miner_power_w': miner_power_consumption_w,
                'energy_cost_per_kwh': energy_cost_per_kwh,
                'daily_revenue_usd': daily_revenue,
                'daily_energy_cost_usd': daily_energy_cost,
                'daily_profit_usd': daily_profit,
                'profit_margin_percentage': (daily_profit / daily_revenue * 100) if daily_revenue > 0 else 0,
                'roi_days': roi_days,
                'break_even_days': roi_days,
                'monthly_profit_usd': daily_profit * 30,
                'annual_profit_usd': daily_profit * 365,
            }

            return profitability

        except Exception as e:
            self.logger.error(f"❌ Erro ao analisar lucratividade: {e}")
            print(f"❌ Erro ao analisar lucratividade: {e}")
            return {}

    def save_to_database(self, data, mining_metrics, profitability):
        """
        ✅ VERSÃO CORRIGIDA - Salva dados no banco DuckDB
        """
        if not self.enable_database or not self.db_manager:
            self.logger.info("📄 Persistência em banco desativada")
            return False

        try:
            # Determinar fonte dos dados
            source = "modular" if self.use_modular_collector else "direct"

            self.logger.info(f"💾 Iniciando salvamento no banco - Fonte: {source}")

            # ✅ PREPARAR DADOS PARA A REDE (schema simplificado)
            network_data = {
                'blocks_24h': data.get('blocks_24h', 0),
                'transactions_24h': data.get('transactions_24h', 0),
                'hashrate_24h': data.get('hashrate_24h', 0),
                'difficulty': data.get('difficulty', 0),
                'market_price_usd': data.get('market_price_usd', 0),
                'mempool_transactions': data.get('mempool_transactions', 0),
                'average_transaction_fee_usd_24h': data.get('average_transaction_fee_usd_24h', 0),
                'nodes': data.get('nodes', 0),
                'blockchain_size': data.get('blockchain_size', 0)
            }

            # ✅ SALVAR MÉTRICAS DE REDE (agora retorna ID)
            network_id = self.db_manager.save_network_metrics(network_data, source)

            if not network_id:
                self.logger.error("❌ Falha crítica ao salvar métricas de rede")
                return False

            self.logger.info(f"✅ Métricas de rede salvas - ID: {network_id}")

            # ✅ SALVAR ANÁLISE DE LUCROTIVIDADE
            if profitability:
                profitability_success = self.db_manager.save_profitability_analysis(
                    profitability,
                    network_id
                )
                if profitability_success:
                    self.logger.info("✅ Análise de lucratividade salva no banco")
                else:
                    self.logger.warning("⚠️ Falha ao salvar análise de lucratividade")

            # ✅ SALVAR SNAPSHOT COMPLETO
            snapshot_data = {
                'timestamp': datetime.now().isoformat(),
                'source': source,
                'metadata': {
                    'success_sources': [source],
                    'network_metrics_id': network_id
                },
                'network_data': data,
                'mining_metrics': mining_metrics,
                'profitability_analysis': profitability
            }

            snapshot_success = self.db_manager.save_comprehensive_snapshot(
                snapshot_data,
                "dashboard_run"
            )

            if snapshot_success:
                self.logger.info("📸 Snapshot completo salvo no banco")

            # ✅ MOSTRAR INFORMAÇÕES DO BANCO
            db_info = self.db_manager.get_database_info()
            if db_info:
                self.logger.info(f"📊 Banco atualizado: {db_info['total_records']} registros totais")

            return True

        except Exception as e:
            self.logger.error(f"❌ Erro ao salvar no banco: {e}")
            return False

    def display_dashboard(self, data, mining_metrics, profitability):
        """
        Exibe o dashboard no console
        """
        if not data:
            print("❌ Nenhum dado para exibir")
            return

        collector_type = "MODULAR" if self.use_modular_collector else "DIRETO"
        db_status = "✅ BANCO" if self.enable_database else "❌ SEM BANCO"

        print("\n" + "="*80)
        print(f"🏭 BITCOIN MINING ANALYTICS DASHBOARD [{collector_type}] [{db_status}]")
        print("="*80)
        print(f"📅 Última atualização: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🔗 Fonte: {'Coletor Modular' if self.use_modular_collector else 'API Blockchair'}")
        print("="*80)

        # Seção: Rede Bitcoin
        print("\n🌐 REDE BITCOIN")
        print("-" * 40)
        print(f"⚡  Hash Rate: {mining_metrics.get('hashrate_ehs', 0):.2f} EH/s")
        print(f"🏗️   Dificuldade: {data.get('difficulty', 0):,.0f}")
        print(f"📦  Blocos (24h): {data.get('blocks_24h', 0)}")
        print(f"💰  Preço BTC: ${data.get('market_price_usd', 0):,.2f}")
        print(f"📊  Transações (24h): {data.get('transactions_24h', 0):,}")
        print(f"📦  Mempool: {data.get('mempool_transactions', 0):,} tx")
        print(f"⏱️   Tempo médio bloco: {mining_metrics.get('block_time_actual', 0):.1f}s")

        # Seção: Mineração
        print("\n⛏️  MINERAÇÃO")
        print("-" * 40)
        print(f"💸  Hash Price: ${mining_metrics.get('hash_price_usd_per_th_per_day', 0):.4f}/TH/dia")
        print(f"📈  Receita diária rede: ${mining_metrics.get('daily_network_revenue_usd', 0):,.0f}")
        print(f"⚡  Consumo energia estimado: {mining_metrics.get('estimated_daily_energy_consumption_gwh', 0):.1f} GWh/dia")

        # Seção: Lucratividade
        if profitability:
            print("\n💰 LUCROTIVIDADE (S19 XP)")
            print("-" * 40)
            print(f"💵  Receita/dia: ${profitability.get('daily_revenue_usd', 0):.2f}")
            print(f"⚡  Custo energia/dia: ${profitability.get('daily_energy_cost_usd', 0):.2f}")
            print(f"📈  Lucro/dia: ${profitability.get('daily_profit_usd', 0):.2f}")
            print(f"📊  Margem: {profitability.get('profit_margin_percentage', 0):.1f}%")
            print(f"🔄  ROI: {profitability.get('roi_days', 0):.0f} dias")

            status = "✅ LUCRO" if profitability.get('daily_profit_usd', 0) > 0 else "❌ PREJUÍZO"
            print(f"🎯  Status: {status}")

        # Seção: Taxas e Mempool
        print("\n💸 TAXAS E MEMPOOL")
        print("-" * 40)
        print(f"📨  Taxa média: {data.get('average_transaction_fee_24h', 0)} sats")
        print(f"💰  Taxa média (USD): ${data.get('average_transaction_fee_usd_24h', 0):.4f}")
        print(f"🔄  TPS: {data.get('mempool_tps', 0):.2f}")

        # Seção: Adoção e Nodes
        print("\n🌍 ADOÇÃO E REDE")
        print("-" * 40)
        print(f"🔗  Nodes: {data.get('nodes', 0):,}")
        print(f"💼  Endereços HODLing: {data.get('hodling_addresses', 0):,}")
        print(f"💾  Tamanho blockchain: {data.get('blockchain_size', 0) / 1e9:.1f} GB")

        print("\n" + "="*80)

    def save_data_backup(self, data):
        """
        Salva backup dos dados em JSON
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            collector_type = "modular" if self.use_modular_collector else "direct"
            filename = f"data/backups/blockchair_snapshot_{timestamp}_{collector_type}.json"

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            self.logger.info(f"✅ Backup salvo: {filename}")
            print(f"✅ Backup salvo: {filename}")
            return filename

        except Exception as e:
            self.logger.error(f"❌ Erro ao salvar backup: {e}")
            print(f"❌ Erro ao salvar backup: {e}")
            return None

    def save_analysis_report(self, data, mining_metrics, profitability):
        """
        Salva relatório de análise
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            collector_type = "modular" if self.use_modular_collector else "direct"
            filename = f"results/financial_analysis/mining_analysis_{timestamp}_{collector_type}.json"

            report = {
                'timestamp': datetime.now().isoformat(),
                'data_source': 'blockchair',
                'collector_type': 'modular' if self.use_modular_collector else 'direct',
                'network_data': data,
                'mining_metrics': mining_metrics,
                'profitability_analysis': profitability
            }

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)

            self.logger.info(f"✅ Relatório de análise salvo: {filename}")
            print(f"✅ Relatório de análise salvo: {filename}")
            return filename

        except Exception as e:
            self.logger.error(f"❌ Erro ao salvar relatório: {e}")
            print(f"❌ Erro ao salvar relatório: {e}")
            return None

    def show_database_info(self):
        """Mostra informações detalhadas do banco de dados"""
        if not self.enable_database or not self.db_manager:
            print("🗄️  Banco de dados: Desativado")
            return

        try:
            db_info = self.db_manager.get_database_info()
            if db_info:
                print(f"\n🗄️  INFORMAÇÕES DO BANCO:")
                print(f"   📁 Arquivo: {db_info['database_path']}")
                print(f"   💾 Tamanho: {db_info['database_size_mb']} MB")
                print(f"   📈 Total de registros: {db_info['total_records']}")

                if db_info['total_records'] > 0:
                    print(f"   ⏰ Período: {db_info['oldest_record']} a {db_info['newest_record']}")

                print(f"   📋 Detalhes por tabela:")
                for table, count in db_info['record_counts'].items():
                    print(f"      🗃️  {table}: {count} registros")

                # Mostrar métricas recentes do banco
                latest_metrics = self.db_manager.get_latest_metrics(1)
                if latest_metrics:
                    metric = latest_metrics[0]
                    print(f"   📊 Última métrica:")
                    print(f"      💰 ${metric['price_usd']:,.0f} | ⚡ {metric['hashrate_ehs']:,.1f} EH/s")
        except Exception as e:
            self.logger.warning(f"⚠️ Não foi possível obter informações do banco: {e}")

    async def run_analysis(self):
        """
        Executa análise completa com DuckDB integrado
        """
        print("🚀 INICIANDO ANÁLISE BITCOIN MINING ANALYTICS")
        print("=" * 60)

        if self.use_modular_collector:
            print("🔧 Modo: Coletor Modular")
        else:
            print("🔧 Modo: API Direta (Fallback)")

        if self.enable_database:
            print("🗄️  Persistência: DuckDB Ativa")
        else:
            print("🗄️  Persistência: Desativada")

        # Coletar dados
        data = await self.fetch_blockchain_data()

        if not data:
            print("❌ Falha na coleta de dados. Verifique conexão e tente novamente.")
            return

        # Processar dados
        mining_metrics = self.calculate_mining_metrics(data)
        profitability = self.analyze_profitability(data, mining_metrics)

        # Exibir dashboard
        self.display_dashboard(data, mining_metrics, profitability)

        # Salvar dados
        backup_file = self.save_data_backup(data)
        report_file = self.save_analysis_report(data, mining_metrics, profitability)

        # ✅ SALVAR NO BANCO DE DADOS (AGORA FUNCIONAL)
        db_success = False
        if self.enable_database:
            db_success = self.save_to_database(data, mining_metrics, profitability)
        else:
            print("📄 Dados salvos apenas em arquivos JSON")

        # Mostrar informações do banco
        self.show_database_info()

        # Resumo final
        print("\n📊 RESUMO DA EXECUÇÃO:")
        print(f"✅ Dados coletados: {len(data)} campos")
        print(f"✅ Método: {'Coletor Modular' if self.use_modular_collector else 'API Direta'}")

        if self.enable_database:
            if db_success:
                print("✅ Persistência: Banco DuckDB (dados salvos)")
            else:
                print("❌ Persistência: Banco DuckDB (falha no salvamento)")
        else:
            print("✅ Persistência: Apenas arquivos JSON")

        if backup_file:
            print(f"✅ Backup salvo: {backup_file}")
        if report_file:
            print(f"✅ Relatório salvo: {report_file}")

        print(f"✅ Logs: logs/dashboard.log")

        print("\n🎯 PRÓXIMOS PASSOS:")
        if self.enable_database and db_success:
            print("   📊 Execute: python scripts/bitcoin_dashboard.py (para visualizar dados)")
            print("   🔄 Execute novamente para adicionar mais dados ao banco")
        else:
            print("   🔄 Execute novamente para dados atualizados")

        print("   📈 Explore 'notebooks/01_data_collection/' para análises detalhadas")

        print("\n" + "="*60)
        print("✅ ANÁLISE CONCLUÍDA COM SUCESSO!")

def main():
    """
    Função principal para execução do script
    """
    try:
        # Verificar se deve usar coletor modular
        use_modular = COLLECTOR_AVAILABLE
        enable_db = DB_AVAILABLE

        if not COLLECTOR_AVAILABLE:
            print("⚠️  Coletor modular não disponível, usando API direta")

        if not DB_AVAILABLE:
            print("⚠️  Banco DuckDB não disponível, usando apenas arquivos")

        dashboard = BitcoinBlockchairDashboard(
            use_modular_collector=use_modular,
            enable_database=enable_db
        )

        # Executar análise assíncrona
        asyncio.run(dashboard.run_analysis())

    except KeyboardInterrupt:
        print("\n⏹️  Execução interrompida pelo usuário")
    except Exception as e:
        print(f"❌ Erro na execução: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
