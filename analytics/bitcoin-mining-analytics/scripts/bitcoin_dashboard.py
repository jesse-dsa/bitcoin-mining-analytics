# scripts/bitcoin_dashboard.py
import sys
import os
import pandas as pd
from datetime import datetime, timedelta
import logging

# Adicionar diretório pai ao path para imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from database.duckdb_manager import DuckDBManager
    DB_AVAILABLE = True
    print("✅ Gerenciador de banco DuckDB carregado")
except ImportError as e:
    print(f"❌ Gerenciador de banco não disponível: {e}")
    DB_AVAILABLE = False

class BitcoinDashboard:
    """Dashboard interativo completo para Bitcoin Mining Analytics"""

    def __init__(self):
        if not DB_AVAILABLE:
            print("❌ Banco de dados não disponível. Execute o coletor primeiro.")
            sys.exit(1)

        self.db = DuckDBManager()
        self.setup_logging()

    def setup_logging(self):
        """Configura o sistema de logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger(__name__)

    def show_main_dashboard(self):
        """Mostra dashboard principal com resumo completo"""
        print("\n" + "="*80)
        print("🎯 BITCOIN MINING ANALYTICS DASHBOARD")
        print("="*80)

        # Informações do banco
        info = self.db.get_database_info()

        print(f"\n📊 INFORMAÇÕES DO BANCO:")
        print(f"   📁 Arquivo: {info['database_path']}")
        print(f"   💾 Tamanho: {info['database_size_mb']} MB")
        print(f"   📈 Total de registros: {info['total_records']}")

        if info['total_records'] > 0:
            print(f"   📅 Período: {info['oldest_record']} a {info['newest_record']}")

        print(f"\n📋 TABELAS:")
        for table, count in info['record_counts'].items():
            print(f"   🗃️  {table:25} : {count:4} registros")

        # Métricas mais recentes
        if info['record_counts']['bitcoin_network_metrics'] > 0:
            self.show_recent_metrics()

        # Análise de lucratividade atual
        if info['record_counts']['profitability_analysis'] > 0:
            self.show_current_profitability()

        # Estatísticas rápidas
        self.show_quick_stats()

    def show_recent_metrics(self, limit: int = 5):
        """Mostra métricas recentes da rede"""
        print(f"\n🔍 MÉTRICAS RECENTES DA REDE (últimos {limit} registros):")
        print("-" * 80)

        metrics = self.db.get_latest_metrics(limit)

        if not metrics:
            print("   ℹ️  Nenhuma métrica disponível")
            return

        for i, metric in enumerate(metrics, 1):
            print(f"{i}. ⏰ {metric['timestamp'].strftime('%Y-%m-%d %H:%M')}")
            print(f"   💰 Preço: ${metric['price_usd']:,.2f}")
            print(f"   ⚡ Hash Rate: {metric['hashrate_ehs']:,.1f} EH/s")
            print(f"   🏗️  Dificuldade: {metric['difficulty']:,.0f}")
            print(f"   📦 Transações/24h: {metric['transactions_24h']:,}")
            print(f"   📨 Mempool: {metric['mempool_txs']:,} tx")
            print(f"   💸 Taxa média: ${metric['avg_fee_usd']:.4f}")
            print()

    def show_current_profitability(self):
        """Mostra análise de lucratividade atual"""
        print(f"\n💰 ANÁLISE DE LUCROTIVIDADE ATUAL:")
        print("-" * 60)

        try:
            conn = self.db._get_connection()

            # Última análise de lucratividade
            result = conn.execute("""
                SELECT
                    timestamp, miner_model, daily_revenue_usd, daily_energy_cost_usd,
                    daily_profit_usd, profit_margin_percentage, roi_days, status
                FROM profitability_analysis
                ORDER BY timestamp DESC
                LIMIT 1
            """).fetchone()

            conn.close()

            if result:
                timestamp, model, revenue, energy_cost, profit, margin, roi, status = result
                print(f"   ⏰ {timestamp.strftime('%Y-%m-%d %H:%M')}")
                print(f"   ⛏️  Minerador: {model}")
                print(f"   💵 Receita/dia: ${revenue:.2f}")
                print(f"   ⚡ Custo energia/dia: ${energy_cost:.2f}")
                print(f"   📈 Lucro/dia: ${profit:.2f}")
                print(f"   📊 Margem: {margin:.1f}%")
                print(f"   🔄 ROI: {roi:.0f} dias")
                print(f"   🎯 Status: {'✅ LUCRO' if status == 'PROFIT' else '❌ PREJUÍZO'}")
            else:
                print("   ℹ️  Nenhuma análise de lucratividade disponível")

        except Exception as e:
            print(f"   ❌ Erro ao obter lucratividade: {e}")

    def show_quick_stats(self):
        """Mostra estatísticas rápidas do banco"""
        print(f"\n📈 ESTATÍSTICAS RÁPIDAS:")
        print("-" * 50)

        try:
            conn = self.db._get_connection()

            # Preço médio atual
            avg_price = conn.execute("""
                SELECT AVG(market_price_usd)
                FROM bitcoin_network_metrics
                WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
            """).fetchone()[0]

            if avg_price:
                print(f"   💰 Preço médio (1h): ${avg_price:,.2f}")

            # Hash rate médio
            avg_hashrate = conn.execute("""
                SELECT AVG(hashrate_24h_ehs)
                FROM bitcoin_network_metrics
                WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
            """).fetchone()[0]

            if avg_hashrate:
                print(f"   ⚡ Hash rate médio (1h): {avg_hashrate:,.1f} EH/s")

            # Total de transações hoje
            total_txs = conn.execute("""
                SELECT SUM(transactions_24h)
                FROM bitcoin_network_metrics
                WHERE DATE(timestamp) = CURRENT_DATE
            """).fetchone()[0]

            if total_txs:
                print(f"   📊 Transações (hoje): {total_txs:,}")

            conn.close()

        except Exception as e:
            print(f"   ❌ Erro ao calcular estatísticas: {e}")

    def show_historical_trends(self, days: int = 7):
        """Mostra tendências históricas"""
        print(f"\n📈 TENDÊNCIAS HISTÓRICAS (últimos {days} dias):")
        print("-" * 70)

        try:
            conn = self.db._get_connection()

            result = conn.execute("""
                SELECT
                    DATE(timestamp) as date,
                    AVG(market_price_usd) as avg_price,
                    AVG(hashrate_24h_ehs) as avg_hashrate,
                    AVG(difficulty) as avg_difficulty,
                    AVG(transactions_24h) as avg_transactions,
                    COUNT(*) as data_points
                FROM bitcoin_network_metrics
                WHERE timestamp >= CURRENT_DATE - INTERVAL ? DAYS
                GROUP BY DATE(timestamp)
                ORDER BY date DESC
            """, (days,)).fetchall()

            conn.close()

            if result:
                for row in result:
                    date, price, hashrate, difficulty, txs, points = row
                    if price and hashrate:  # Só mostrar se tiver dados válidos
                        print(f"   📅 {date.strftime('%Y-%m-%d')} ({points} pontos):")
                        print(f"      💰 ${price:,.0f} | ⚡ {hashrate:,.1f} EH/s")
                        print(f"      🏗️  {difficulty:,.0f} | 📦 {txs:,.0f} tx/dia")
                        print()
            else:
                print("   ℹ️  Dados insuficientes para análise histórica")

        except Exception as e:
            print(f"   ❌ Erro ao obter tendências: {e}")

    def show_profitability_history(self, limit: int = 10):
        """Mostra histórico de lucratividade"""
        print(f"\n💰 HISTÓRICO DE LUCROTIVIDADE (últimos {limit} registros):")
        print("-" * 70)

        try:
            conn = self.db._get_connection()

            result = conn.execute("""
                SELECT
                    timestamp,
                    daily_profit_usd,
                    profit_margin_percentage,
                    roi_days,
                    status
                FROM profitability_analysis
                ORDER BY timestamp DESC
                LIMIT ?
            """, (limit,)).fetchall()

            conn.close()

            if result:
                for i, row in enumerate(result, 1):
                    timestamp, profit, margin, roi, status = row
                    status_icon = "✅" if status == "PROFIT" else "❌"
                    print(f"{i}. {timestamp.strftime('%m/%d %H:%M')} | "
                        f"${profit:.2f}/dia | {margin:.1f}% | "
                        f"ROI: {roi:.0f}d {status_icon}")
            else:
                print("   ℹ️  Nenhum dado de lucratividade disponível")

        except Exception as e:
            print(f"   ❌ Erro ao obter histórico de lucratividade: {e}")

    def explore_table_data(self, table_name: str, limit: int = 10):
        """Explora dados de uma tabela específica"""
        print(f"\n📖 EXPLORANDO: {table_name.upper()} (últimos {limit} registros)")
        print("-" * 70)

        df = self.db.export_to_dataframe(table_name, limit)

        if df.empty:
            print("   ❌ Tabela vazia ou não encontrada")
            return

        print(f"   📊 Total de registros na tabela: {len(self.db.export_to_dataframe(table_name))}")
        print(f"   🎯 Mostrando {len(df)} registros mais recentes")

        # Formatação específica para cada tabela
        if table_name == 'bitcoin_network_metrics':
            display_cols = ['timestamp', 'data_source', 'hashrate_24h_ehs', 'market_price_usd', 'transactions_24h']
            if all(col in df.columns for col in display_cols):
                display_df = df[display_cols].head(limit)
                print("\n" + display_df.to_string(
                    index=False,
                    formatters={
                        'timestamp': lambda x: x.strftime('%m/%d %H:%M'),
                        'hashrate_24h_ehs': lambda x: f"{x:,.1f}",
                        'market_price_usd': lambda x: f"${x:,.0f}",
                        'transactions_24h': lambda x: f"{x:,}"
                    }
                ))

        elif table_name == 'profitability_analysis':
            display_cols = ['timestamp', 'miner_model', 'daily_profit_usd', 'profit_margin_percentage', 'status']
            if all(col in df.columns for col in display_cols):
                display_df = df[display_cols].head(limit)
                print("\n" + display_df.to_string(
                    index=False,
                    formatters={
                        'timestamp': lambda x: x.strftime('%m/%d %H:%M'),
                        'daily_profit_usd': lambda x: f"${x:.2f}",
                        'profit_margin_percentage': lambda x: f"{x:.1f}%"
                    }
                ))

        else:
            # Mostrar todas as colunas para outras tabelas
            print("\n" + df.head(limit).to_string(index=False))

    def show_table_schema(self, table_name: str):
        """Mostra schema de uma tabela"""
        print(f"\n🏗️  SCHEMA DA TABELA: {table_name.upper()}")
        print("-" * 60)

        try:
            conn = self.db._get_connection()
            schema = conn.execute(f"DESCRIBE {table_name}").fetchall()
            conn.close()

            if schema:
                for col in schema:
                    nullable = "NULL" if col[2] == "YES" else "NOT NULL"
                    print(f"   {col[0]:25} {col[1]:15} {nullable}")
            else:
                print("   ❌ Tabela não encontrada")

        except Exception as e:
            print(f"   ❌ Erro ao obter schema: {e}")

    def export_data(self):
        """Exporta dados para análise externa"""
        print(f"\n💾 EXPORTAR DADOS:")
        print("-" * 50)

        tables = {
            '1': ('bitcoin_network_metrics', 'Métricas de Rede Bitcoin'),
            '2': ('profitability_analysis', 'Análise de Lucratividade'),
            '3': ('bitcoin_snapshots', 'Snapshots Completos')
        }

        for key, (table, description) in tables.items():
            print(f"   {key}. {description}")

        choice = input("\nEscolha a tabela para exportar (1-3): ").strip()

        if choice in tables:
            table_name, description = tables[choice]

            # Exportar para DataFrame
            df = self.db.export_to_dataframe(table_name)

            if not df.empty:
                # Criar diretório de exports se não existir
                os.makedirs('exports', exist_ok=True)

                # Nome do arquivo com timestamp
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"exports/{table_name}_{timestamp}.csv"

                # Exportar para CSV
                df.to_csv(filename, index=False)

                print(f"\n✅ Dados exportados com sucesso!")
                print(f"   📁 Arquivo: {filename}")
                print(f"   📊 Registros: {len(df)}")
                print(f"   📋 Colunas: {len(df.columns)}")

                # Mostrar preview
                print(f"\n🎯 Preview dos dados (primeiras 3 linhas):")
                print(df.head(3).to_string(index=False))

                # Estatísticas básicas para métricas numéricas
                if table_name == 'bitcoin_network_metrics':
                    numeric_cols = df.select_dtypes(include=['number']).columns
                    if len(numeric_cols) > 0:
                        print(f"\n📈 Estatísticas básicas:")
                        for col in numeric_cols[:3]:  # Mostrar apenas 3 colunas
                            if col in ['market_price_usd', 'hashrate_24h_ehs']:
                                print(f"   {col}: min={df[col].min():.0f}, max={df[col].max():.0f}, avg={df[col].mean():.0f}")
            else:
                print(f"❌ Tabela {table_name} está vazia")
        else:
            print("❌ Escolha inválida")

    def show_database_health(self):
        """Mostra saúde e integridade do banco"""
        print(f"\n🔧 SAÚDE DO BANCO DE DADOS:")
        print("-" * 50)

        try:
            info = self.db.get_database_info()

            # Verificar integridade básica
            checks = {
                '📁 Arquivo existe': os.path.exists(info['database_path']),
                '💾 Tamanho adequado': info['database_size_mb'] > 0,
                '📊 Tem dados': info['total_records'] > 0,
                '🕒 Dados recentes': info['newest_record'] > datetime.now() - timedelta(hours=24) if info['newest_record'] else False
            }

            for check, status in checks.items():
                icon = "✅" if status else "❌"
                print(f"   {icon} {check}")

            # Recomendações
            print(f"\n💡 RECOMENDAÇÕES:")
            if info['total_records'] < 10:
                print("   🔄 Execute o coletor mais vezes para acumular dados")
            if not checks['🕒 Dados recentes']:
                print("   ⏰ Execute o coletor para dados atualizados")
            if info['database_size_mb'] > 100:
                print("   🧹 Considere fazer backup e limpar dados antigos")

        except Exception as e:
            print(f"   ❌ Erro ao verificar saúde do banco: {e}")

    def interactive_mode(self):
        """Modo interativo do dashboard"""
        while True:
            print("\n" + "="*60)
            print("🔍 DASHBOARD INTERATIVO - BITCOIN MINING ANALYTICS")
            print("="*60)
            print("1. 📊 Dashboard Principal")
            print("2. 🔍 Métricas Recentes da Rede")
            print("3. 💰 Lucratividade Atual")
            print("4. 📈 Tendências Históricas")
            print("5. 💸 Histórico de Lucratividade")
            print("6. 🗃️  Explorar Tabelas")
            print("7. 🏗️  Ver Schemas")
            print("8. 💾 Exportar Dados")
            print("9. 🔧 Saúde do Banco")
            print("0. ↩️  Sair")

            choice = input("\nEscolha uma opção: ").strip()

            if choice == "1":
                self.show_main_dashboard()
            elif choice == "2":
                try:
                    limit = int(input("Número de registros (padrão 5): ") or "5")
                    self.show_recent_metrics(limit)
                except ValueError:
                    print("❌ Número inválido, usando padrão 5")
                    self.show_recent_metrics(5)
            elif choice == "3":
                self.show_current_profitability()
            elif choice == "4":
                try:
                    days = int(input("Número de dias (padrão 7): ") or "7")
                    self.show_historical_trends(days)
                except ValueError:
                    print("❌ Número inválido, usando padrão 7")
                    self.show_historical_trends(7)
            elif choice == "5":
                try:
                    limit = int(input("Número de registros (padrão 10): ") or "10")
                    self.show_profitability_history(limit)
                except ValueError:
                    print("❌ Número inválido, usando padrão 10")
                    self.show_profitability_history(10)
            elif choice == "6":
                self.explore_tables_menu()
            elif choice == "7":
                self.show_schemas_menu()
            elif choice == "8":
                self.export_data()
            elif choice == "9":
                self.show_database_health()
            elif choice == "0":
                print("👋 Encerrando dashboard...")
                break
            else:
                print("❌ Opção inválida")

    def explore_tables_menu(self):
        """Menu para explorar tabelas específicas"""
        tables = {
            '1': ('bitcoin_network_metrics', 'Métricas de Rede'),
            '2': ('profitability_analysis', 'Análise de Lucratividade'),
            '3': ('bitcoin_snapshots', 'Snapshots Completos')
        }

        print(f"\n🗃️  EXPLORAR TABELAS:")
        print("-" * 40)

        for key, (table, description) in tables.items():
            print(f"   {key}. {description}")
        print("   4. ↩️  Voltar")

        choice = input("\nEscolha tabela para explorar (1-4): ").strip()

        if choice in tables:
            table_name, description = tables[choice]
            try:
                limit = int(input("Número de registros (padrão 10): ") or "10")
                self.explore_table_data(table_name, limit)
            except ValueError:
                print("❌ Número inválido, usando padrão 10")
                self.explore_table_data(table_name, 10)
        elif choice == "4":
            return
        else:
            print("❌ Opção inválida")

    def show_schemas_menu(self):
        """Menu para mostrar schemas"""
        tables = ['bitcoin_network_metrics', 'profitability_analysis', 'bitcoin_snapshots']

        print(f"\n🏗️  VER SCHEMAS:")
        print("-" * 40)

        for i, table in enumerate(tables, 1):
            print(f"   {i}. {table}")
        print("   4. ↩️  Voltar")

        choice = input("\nEscolha tabela para ver schema (1-4): ").strip()

        if choice in ['1', '2', '3']:
            table_name = tables[int(choice) - 1]
            self.show_table_schema(table_name)
        elif choice == "4":
            return
        else:
            print("❌ Opção inválida")

def main():
    """Função principal"""
    print("🚀 INICIANDO BITCOIN MINING ANALYTICS DASHBOARD")

    if not DB_AVAILABLE:
        print("❌ Banco de dados não disponível.")
        print("   Execute primeiro: python scripts/bitcoin_blockchair_dashboard.py")
        return

    try:
        dashboard = BitcoinDashboard()

        # Mostrar dashboard principal
        dashboard.show_main_dashboard()

        # Entrar no modo interativo
        dashboard.interactive_mode()

    except Exception as e:
        print(f"❌ Erro ao iniciar dashboard: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
