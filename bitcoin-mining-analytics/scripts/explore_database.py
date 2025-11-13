# scripts/explore_database.py
import sys
import os
import pandas as pd
from datetime import datetime
import duckdb

# Adicionar diretório pai ao path para imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from database.duckdb_manager import DuckDBManager
    DB_AVAILABLE = True
except ImportError as e:
    print(f"❌ Gerenciador de banco não disponível: {e}")
    DB_AVAILABLE = False

class DatabaseExplorer:
    """Explorador visual completo da base de dados"""

    def __init__(self):
        if not DB_AVAILABLE:
            print("❌ Banco de dados não disponível.")
            sys.exit(1)

        self.db = DuckDBManager()
        self.conn = duckdb.connect("data/bitcoin_analytics.duckdb")

    def show_database_overview(self):
        """Mostra visão geral completa do banco"""
        print("\n" + "="*80)
        print("🗃️  VISÃO GERAL DA BASE DE DADOS")
        print("="*80)

        # Informações básicas
        info = self.db.get_database_info()
        print(f"📁 Arquivo: {info['database_path']}")
        print(f"💾 Tamanho: {info['database_size_mb']} MB")
        print(f"📊 Total de registros: {info['total_records']}")
        print(f"📅 Período: {info['oldest_record']} a {info['newest_record']}")

        print(f"\n📋 ESTRUTURA DO BANCO:")
        for table, count in info['record_counts'].items():
            print(f"   🗂️  {table}: {count} registros")

        # Estatísticas de uso
        self.show_usage_statistics()

    def show_usage_statistics(self):
        """Mostra estatísticas de uso do banco"""
        print(f"\n📈 ESTATÍSTICAS DE USO:")

        try:
            # Frequência de coleta
            freq_result = self.conn.execute("""
                SELECT
                    COUNT(*) as total_entries,
                    MIN(timestamp) as first_entry,
                    MAX(timestamp) as last_entry,
                    COUNT(DISTINCT DATE(timestamp)) as days_with_data
                FROM bitcoin_network_metrics
            """).fetchone()

            if freq_result:
                total, first, last, days = freq_result
                if total > 0:
                    hours_covered = (last - first).total_seconds() / 3600
                    avg_per_hour = total / max(1, hours_covered)

                    print(f"   ⏰ Período coberto: {hours_covered:.1f} horas")
                    print(f"   📊 Média: {avg_per_hour:.1f} registros/hora")
                    print(f"   📅 Dias com dados: {days}")

            # Fontes de dados
            sources = self.conn.execute("""
                SELECT data_source, COUNT(*) as count
                FROM bitcoin_network_metrics
                GROUP BY data_source
                ORDER BY count DESC
            """).fetchall()

            if sources:
                print(f"   🔗 Fontes de dados:")
                for source, count in sources:
                    print(f"      {source}: {count} registros")

        except Exception as e:
            print(f"   ❌ Erro nas estatísticas: {e}")

    def show_table_details(self, table_name):
        """Mostra detalhes completos de uma tabela"""
        print(f"\n📖 DETALHES DA TABELA: {table_name.upper()}")
        print("-" * 70)

        try:
            # Schema
            schema = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            print("🏗️  SCHEMA:")
            for col in schema:
                nullable = "NULL" if col[2] == "YES" else "NOT NULL"
                print(f"   {col[0]:25} {col[1]:15} {nullable}")

            # Contagem
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"\n📊 TOTAL DE REGISTROS: {count}")

            # Período dos dados
            if 'timestamp' in [col[0] for col in schema]:
                time_range = self.conn.execute(f"""
                    SELECT MIN(timestamp), MAX(timestamp)
                    FROM {table_name}
                """).fetchone()
                if time_range[0]:
                    print(f"📅 PERÍODO: {time_range[0]} a {time_range[1]}")

            # Amostra dos dados
            if count > 0:
                print(f"\n🎯 AMOSTRA DOS DADOS (5 primeiros registros):")
                sample = self.conn.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchall()
                columns = [desc[0] for desc in self.conn.description]

                # Mostrar colunas
                print("   " + " | ".join(columns))
                print("   " + "-" * 80)

                # Mostrar dados
                for row in sample:
                    formatted_row = []
                    for i, value in enumerate(row):
                        if columns[i] == 'timestamp' and value:
                            formatted_row.append(value.strftime('%m/%d %H:%M'))
                        elif isinstance(value, (int, float)) and value > 1000:
                            formatted_row.append(f"{value:,}")
                        elif isinstance(value, float):
                            formatted_row.append(f"{value:.2f}")
                        else:
                            formatted_row.append(str(value))

                    print("   " + " | ".join(formatted_row))

        except Exception as e:
            print(f"❌ Erro ao acessar tabela {table_name}: {e}")

    def show_network_metrics_analysis(self):
        """Análise detalhada das métricas de rede"""
        print(f"\n🌐 ANÁLISE DAS MÉTRICAS DE REDE")
        print("-" * 70)

        try:
            # Estatísticas básicas
            stats = self.conn.execute("""
                SELECT
                    COUNT(*) as total_records,
                    AVG(market_price_usd) as avg_price,
                    AVG(hashrate_24h_ehs) as avg_hashrate,
                    AVG(transactions_24h) as avg_transactions,
                    AVG(difficulty) as avg_difficulty,
                    MIN(market_price_usd) as min_price,
                    MAX(market_price_usd) as max_price,
                    MIN(hashrate_24h_ehs) as min_hashrate,
                    MAX(hashrate_24h_ehs) as max_hashrate
                FROM bitcoin_network_metrics
            """).fetchone()

            if stats and stats[0] > 0:
                total, avg_price, avg_hash, avg_tx, avg_diff, min_p, max_p, min_h, max_h = stats

                print("📊 ESTATÍSTICAS GERAIS:")
                print(f"   💰 Preço BTC: ${min_p:,.0f} - ${max_p:,.0f} (média: ${avg_price:,.0f})")
                print(f"   ⚡ Hash Rate: {min_h:,.1f} - {max_h:,.1f} EH/s (média: {avg_hash:,.1f} EH/s)")
                print(f"   📦 Transações/dia: {avg_tx:,.0f} (média)")
                print(f"   🏗️  Dificuldade: {avg_diff:,.0f} (média)")

            # Evolução temporal
            print(f"\n📈 EVOLUÇÃO TEMPORAL:")
            evolution = self.conn.execute("""
                SELECT
                    DATE(timestamp) as date,
                    COUNT(*) as records,
                    AVG(market_price_usd) as avg_price,
                    AVG(hashrate_24h_ehs) as avg_hashrate
                FROM bitcoin_network_metrics
                GROUP BY DATE(timestamp)
                ORDER BY date DESC
                LIMIT 10
            """).fetchall()

            if evolution:
                for date, records, price, hashrate in evolution:
                    if price and hashrate:
                        print(f"   📅 {date}: {records} registros | ${price:,.0f} | {hashrate:,.1f} EH/s")

        except Exception as e:
            print(f"❌ Erro na análise: {e}")

    def show_profitability_analysis(self):
        """Análise detalhada da lucratividade"""
        print(f"\n💰 ANÁLISE DE LUCROTIVIDADE")
        print("-" * 70)

        try:
            # Estatísticas de lucratividade
            profit_stats = self.conn.execute("""
                SELECT
                    COUNT(*) as total_analysis,
                    AVG(daily_profit_usd) as avg_profit,
                    AVG(profit_margin_percentage) as avg_margin,
                    AVG(roi_days) as avg_roi,
                    SUM(CASE WHEN status = 'PROFIT' THEN 1 ELSE 0 END) as profitable_count,
                    SUM(CASE WHEN status = 'LOSS' THEN 1 ELSE 0 END) as loss_count
                FROM profitability_analysis
            """).fetchone()

            if profit_stats and profit_stats[0] > 0:
                total, avg_profit, avg_margin, avg_roi, profit_count, loss_count = profit_stats
                profit_percentage = (profit_count / total) * 100 if total > 0 else 0

                print("📊 ESTATÍSTICAS DE LUCRO:")
                print(f"   💵 Lucro médio/dia: ${avg_profit:.2f}")
                print(f"   📊 Margem média: {avg_margin:.1f}%")
                print(f"   🔄 ROI médio: {avg_roi:.0f} dias")
                print(f"   📈 Rentabilidade: {profit_count}/{total} ({profit_percentage:.1f}%)")

            # Tendência de lucratividade
            print(f"\n📈 TENDÊNCIA DE LUCROTIVIDADE:")
            trend = self.conn.execute("""
                SELECT
                    DATE(timestamp) as date,
                    AVG(daily_profit_usd) as avg_daily_profit,
                    AVG(profit_margin_percentage) as avg_margin
                FROM profitability_analysis
                GROUP BY DATE(timestamp)
                ORDER BY date DESC
                LIMIT 7
            """).fetchall()

            if trend:
                for date, avg_profit, avg_margin in trend:
                    if avg_profit is not None:
                        status = "✅" if avg_profit > 0 else "❌"
                        print(f"   {status} {date}: ${avg_profit:.2f} | {avg_margin:.1f}%")

        except Exception as e:
            print(f"❌ Erro na análise de lucratividade: {e}")

    def show_data_quality_report(self):
        """Relatório de qualidade dos dados"""
        print(f"\n🔍 RELATÓRIO DE QUALIDADE DOS DADOS")
        print("-" * 70)

        try:
            # Verificar completude dos dados
            completeness = self.conn.execute("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN market_price_usd IS NULL THEN 1 ELSE 0 END) as null_prices,
                    SUM(CASE WHEN hashrate_24h_ehs IS NULL THEN 1 ELSE 0 END) as null_hashrates,
                    SUM(CASE WHEN transactions_24h IS NULL THEN 1 ELSE 0 END) as null_transactions
                FROM bitcoin_network_metrics
            """).fetchone()

            if completeness:
                total, null_prices, null_hashes, null_txs = completeness

                print("📊 COMPLETUDE DOS DADOS:")
                print(f"   ✅ Registros totais: {total}")
                print(f"   ❌ Preços nulos: {null_prices} ({(null_prices/total)*100:.1f}%)")
                print(f"   ❌ Hash rates nulos: {null_hashes} ({(null_hashes/total)*100:.1f}%)")
                print(f"   ❌ Transações nulas: {null_txs} ({(null_txs/total)*100:.1f}%)")

            # Verificar consistência temporal
            temporal_gaps = self.conn.execute("""
                WITH time_gaps AS (
                    SELECT
                        timestamp,
                        LAG(timestamp) OVER (ORDER BY timestamp) as prev_timestamp,
                        EXTRACT(EPOCH FROM (timestamp - LAG(timestamp) OVER (ORDER BY timestamp))) as gap_seconds
                    FROM bitcoin_network_metrics
                    ORDER BY timestamp
                )
                SELECT
                    AVG(gap_seconds) as avg_gap,
                    MAX(gap_seconds) as max_gap,
                    MIN(gap_seconds) as min_gap
                FROM time_gaps
                WHERE gap_seconds IS NOT NULL
            """).fetchone()

            if temporal_gaps and temporal_gaps[0]:
                avg_gap, max_gap, min_gap = temporal_gaps
                print(f"\n⏰ CONSISTÊNCIA TEMPORAL:")
                print(f"   ⏱️  Intervalo médio: {avg_gap/60:.1f} minutos")
                print(f"   📏 Maior intervalo: {max_gap/60:.1f} minutos")
                print(f"   📐 Menor intervalo: {min_gap/60:.1f} minutos")

        except Exception as e:
            print(f"❌ Erro no relatório de qualidade: {e}")

    def show_detailed_records(self, table_name, limit=10):
        """Mostra registros detalhados de uma tabela"""
        print(f"\n📋 REGISTROS DETALHADOS: {table_name.upper()}")
        print("-" * 80)

        try:
            # Obter todos os registros
            result = self.conn.execute(f"SELECT * FROM {table_name} ORDER BY timestamp DESC LIMIT {limit}").fetchall()
            columns = [desc[0] for desc in self.conn.description]

            if not result:
                print("   ℹ️  Nenhum registro encontrado")
                return

            print(f"   Mostrando {len(result)} registros mais recentes:")
            print()

            for i, row in enumerate(result, 1):
                print(f"🎯 REGISTRO {i}:")
                for j, value in enumerate(row):
                    col_name = columns[j]

                    # Formatação específica por tipo de campo
                    if col_name == 'timestamp' and value:
                        formatted_value = value.strftime('%Y-%m-%d %H:%M:%S')
                    elif col_name.endswith('_usd') and value:
                        formatted_value = f"${value:,.2f}"
                    elif col_name == 'hashrate_24h_ehs' and value:
                        formatted_value = f"{value:,.1f} EH/s"
                    elif col_name == 'difficulty' and value:
                        formatted_value = f"{value:,.0f}"
                    elif isinstance(value, (int, float)) and value > 1000:
                        formatted_value = f"{value:,}"
                    elif isinstance(value, float):
                        formatted_value = f"{value:.4f}"
                    else:
                        formatted_value = str(value)

                    print(f"   {col_name:30}: {formatted_value}")
                print("-" * 50)

        except Exception as e:
            print(f"❌ Erro ao obter registros: {e}")

    def export_complete_database(self):
        """Exporta toda a base de dados para CSV"""
        print(f"\n💾 EXPORTAÇÃO COMPLETA DA BASE")
        print("-" * 60)

        tables = ['bitcoin_network_metrics', 'profitability_analysis', 'bitcoin_snapshots']
        export_dir = "exports/complete_database"

        try:
            os.makedirs(export_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            total_records = 0
            for table in tables:
                df = self.db.export_to_dataframe(table)
                if not df.empty:
                    filename = f"{export_dir}/{table}_{timestamp}.csv"
                    df.to_csv(filename, index=False)
                    print(f"✅ {table}: {len(df)} registros -> {filename}")
                    total_records += len(df)
                else:
                    print(f"ℹ️  {table}: vazia")

            print(f"\n📊 EXPORTAÇÃO CONCLUÍDA:")
            print(f"   📁 Diretório: {export_dir}")
            print(f"   📈 Total de registros: {total_records}")
            print(f"   ⏰ Timestamp: {timestamp}")

        except Exception as e:
            print(f"❌ Erro na exportação: {e}")

    def interactive_explorer(self):
        """Modo interativo de exploração"""
        while True:
            print("\n" + "="*60)
            print("🔍 EXPLORADOR DA BASE DE DADOS")
            print("="*60)
            print("1. 📊 Visão Geral do Banco")
            print("2. 🌐 Análise das Métricas de Rede")
            print("3. 💰 Análise de Lucratividade")
            print("4. 🔍 Relatório de Qualidade")
            print("5. 🗂️  Detalhes das Tabelas")
            print("6. 📋 Registros Detalhados")
            print("7. 💾 Exportar Base Completa")
            print("8. 🎯 Consultas Personalizadas")
            print("0. ↩️  Sair")

            choice = input("\nEscolha uma opção: ").strip()

            if choice == "1":
                self.show_database_overview()
            elif choice == "2":
                self.show_network_metrics_analysis()
            elif choice == "3":
                self.show_profitability_analysis()
            elif choice == "4":
                self.show_data_quality_report()
            elif choice == "5":
                self.show_tables_menu()
            elif choice == "6":
                self.show_records_menu()
            elif choice == "7":
                self.export_complete_database()
            elif choice == "8":
                self.custom_query_menu()
            elif choice == "0":
                print("👋 Encerrando explorador...")
                break
            else:
                print("❌ Opção inválida")

    def show_tables_menu(self):
        """Menu para mostrar detalhes das tabelas"""
        tables = ['bitcoin_network_metrics', 'profitability_analysis', 'bitcoin_snapshots']

        print(f"\n🗂️  DETALHES DAS TABELAS:")
        print("-" * 40)

        for i, table in enumerate(tables, 1):
            print(f"   {i}. {table}")
        print("   4. ↩️  Voltar")

        choice = input("\nEscolha tabela para detalhes (1-4): ").strip()

        if choice in ['1', '2', '3']:
            table_name = tables[int(choice) - 1]
            self.show_table_details(table_name)
        elif choice == "4":
            return
        else:
            print("❌ Opção inválida")

    def show_records_menu(self):
        """Menu para mostrar registros detalhados"""
        tables = ['bitcoin_network_metrics', 'profitability_analysis', 'bitcoin_snapshots']

        print(f"\n📋 REGISTROS DETALHADOS:")
        print("-" * 40)

        for i, table in enumerate(tables, 1):
            print(f"   {i}. {table}")
        print("   4. ↩️  Voltar")

        choice = input("\nEscolha tabela (1-4): ").strip()

        if choice in ['1', '2', '3']:
            table_name = tables[int(choice) - 1]
            try:
                limit = int(input("Número de registros (padrão 10): ") or "10")
                self.show_detailed_records(table_name, limit)
            except ValueError:
                print("❌ Número inválido, usando padrão 10")
                self.show_detailed_records(table_name, 10)
        elif choice == "4":
            return
        else:
            print("❌ Opção inválida")

    def custom_query_menu(self):
        """Menu para consultas personalizadas"""
        print(f"\n🎯 CONSULTAS PERSONALIZADAS")
        print("-" * 50)
        print("Exemplos de consultas:")
        print("  SELECT * FROM bitcoin_network_metrics LIMIT 5")
        print("  SELECT timestamp, market_price_usd FROM bitcoin_network_metrics ORDER BY timestamp DESC LIMIT 10")
        print("  SELECT AVG(market_price_usd) as avg_price FROM bitcoin_network_metrics")
        print()

        query = input("Digite sua consulta SQL: ").strip()

        if not query:
            print("❌ Consulta vazia")
            return

        try:
            if query.lower().startswith('select'):
                result = self.conn.execute(query).fetchall()
                columns = [desc[0] for desc in self.conn.description]

                if result:
                    print(f"\n✅ Resultados ({len(result)} registros):")
                    print("   " + " | ".join(columns))
                    print("   " + "-" * 80)

                    for row in result:
                        formatted_row = []
                        for i, value in enumerate(row):
                            if columns[i] == 'timestamp' and value:
                                formatted_row.append(value.strftime('%m/%d %H:%M'))
                            elif isinstance(value, (int, float)) and value > 1000:
                                formatted_row.append(f"{value:,}")
                            elif isinstance(value, float):
                                formatted_row.append(f"{value:.2f}")
                            else:
                                formatted_row.append(str(value)[:30])  # Limitar tamanho

                        print("   " + " | ".join(formatted_row))
                else:
                    print("ℹ️  Nenhum resultado encontrado")
            else:
                print("❌ Apenas consultas SELECT são permitidas")

        except Exception as e:
            print(f"❌ Erro na consulta: {e}")

def main():
    """Função principal"""
    print("🚀 EXPLORADOR DA BASE DE DADOS BITCOIN MINING ANALYTICS")

    if not DB_AVAILABLE:
        print("❌ Banco de dados não disponível.")
        print("   Execute primeiro: python scripts/bitcoin_blockchair_dashboard.py")
        return

    try:
        explorer = DatabaseExplorer()

        # Mostrar visão geral inicial
        explorer.show_database_overview()

        # Entrar no modo interativo
        explorer.interactive_explorer()

    except Exception as e:
        print(f"❌ Erro no explorador: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'explorer' in locals():
            explorer.conn.close()

if __name__ == "__main__":
    main()
