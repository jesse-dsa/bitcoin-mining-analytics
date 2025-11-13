# scripts/dynamic_analysis.py
import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from database.duckdb_manager import DuckDBManager
    from portfolio_manager import PortfolioManager
    DB_AVAILABLE = True
except ImportError as e:
    print(f"❌ Módulos não disponíveis: {e}")
    DB_AVAILABLE = False

class DynamicPortfolioAnalysis:
    """Análise dinâmica para múltiplos portfolios"""

    def __init__(self):
        if not DB_AVAILABLE:
            print("❌ Módulos não disponíveis.")
            sys.exit(1)

        self.db = DuckDBManager()
        self.portfolio_manager = PortfolioManager()

    def analyze_all_portfolios(self):
        """Analisa todos os portfolios"""
        print(f"\n📊 ANÁLISE DE TODOS OS PORTFOLIOS")
        print("=" * 70)

        portfolios = self.portfolio_manager.list_portfolios()

        if not portfolios:
            print("   ℹ️  Nenhum portfolio encontrado")
            print("   💡 Execute: python portfolio_manager.py para criar portfolios")
            return

        total_combined_value = 0
        portfolio_count = 0

        for portfolio_id, portfolio_data in portfolios.items():
            portfolio_count += 1
            print(f"\n🎯 PORTFOLIO: {portfolio_data['name']}")
            print(f"   👤 Dono: {portfolio_data['owner']}")
            print(f"   💰 Investimento Inicial: ${portfolio_data['initial_investment']:,.2f}")
            print("-" * 50)

            portfolio_value = self._analyze_single_portfolio(portfolio_data)
            total_combined_value += portfolio_value

        print(f"\n{'='*70}")
        print(f"📈 RESUMO GERAL:")
        print(f"   • Portfolios analisados: {portfolio_count}")
        print(f"   • Valor total combinado: ${total_combined_value:,.2f}")

    def _analyze_single_portfolio(self, portfolio_data):
        """Analisa um portfolio individual"""
        try:
            # Preços atuais do mercado (em produção, usar API real)
            current_prices = self._get_current_prices()

            allocations = portfolio_data.get("allocations", {})
            total_invested = portfolio_data["initial_investment"]
            total_current_value = 0

            print("   📊 COMPOSIÇÃO:")
            print("   " + "-" * 45)

            for asset, allocation in allocations.items():
                amount = allocation["amount"]
                avg_price = allocation.get("average_price", 0)
                current_price = current_prices.get(asset, 0)

                current_value = amount
                total_current_value += current_value
                allocation_pct = (amount / total_invested) * 100

                # Calcular P&L se tivermos preço médio
                if avg_price > 0 and current_price > 0:
                    pl_percentage = ((current_price - avg_price) / avg_price) * 100
                    pl_status = f"({pl_percentage:+.1f}%)"
                else:
                    pl_status = ""

                print(f"   {asset:<6} ${amount:>8,.2f} ({allocation_pct:5.1f}%) {pl_status}")

            # Performance geral
            total_pl = total_current_value - total_invested
            total_pl_pct = (total_pl / total_invested) * 100

            print("   " + "-" * 45)
            print(f"   💰 VALOR ATUAL: ${total_current_value:,.2f}")
            print(f"   📈 LUCRO/PREJUÍZO: ${total_pl:+,.2f} ({total_pl_pct:+.1f}%)")

            # Recomendação baseada na performance
            if total_pl_pct > 20:
                print("   🎯 STATUS: 🟢 EXCELENTE - Portfolio em alta")
            elif total_pl_pct > 0:
                print("   🎯 STATUS: 🟡 BOM - Portfolio positivo")
            elif total_pl_pct > -10:
                print("   🎯 STATUS: 🟡 NEUTRO - Pequena correção")
            else:
                print("   🎯 STATUS: 🔴 ATENÇÃO - Portfolio em baixa")

            return total_current_value

        except Exception as e:
            print(f"   ❌ Erro na análise: {e}")
            return 0

    def _get_current_prices(self):
        """Obtém preços atuais (simulados - em produção usar API)"""
        try:
            metrics = self.db.get_latest_metrics(1)
            if metrics:
                btc_price = metrics[0]['price_usd']
                # Preços relativos (em produção, buscar de API real)
                return {
                    'BTC': btc_price,
                    'ETH': btc_price * 0.05,  # Aproximação
                    'LINK': btc_price * 0.0003  # Aproximação
                }
        except:
            pass

        # Fallback para preços padrão
        return {
            'BTC': 65000,
            'ETH': 3250,
            'LINK': 19.5
        }

    def analyze_specific_portfolio(self, portfolio_id: str):
        """Analisa um portfolio específico em detalhes"""
        portfolio = self.portfolio_manager.get_portfolio(portfolio_id)

        if not portfolio:
            print(f"❌ Portfolio '{portfolio_id}' não encontrado")
            return

        print(f"\n🎯 ANÁLISE DETALHADA - {portfolio['name'].upper()}")
        print("=" * 70)
        print(f"   👤 Dono: {portfolio['owner']}")
        print(f"   📅 Criado em: {portfolio['created_at'][:10]}")
        print(f"   💰 Investimento Inicial: ${portfolio['initial_investment']:,.2f}")

        self._analyze_single_portfolio(portfolio)

def main():
    """Análise dinâmica de portfolios"""
    print("🚀 SISTEMA DINÂMICO DE ANÁLISE DE PORTFOLIOS")

    if not DB_AVAILABLE:
        print("❌ Módulos não disponíveis.")
        return

    try:
        analyzer = DynamicPortfolioAnalysis()

        # Analisar TODOS os portfolios
        analyzer.analyze_all_portfolios()

        # Exemplo: Analisar portfolio específico
        # analyzer.analyze_specific_portfolio("você_principal")
        # analyzer.analyze_specific_portfolio("carlos_trading")

        print(f"\n💡 COMANDOS ÚTEIS:")
        print("   • python portfolio_manager.py - Gerenciar portfolios")
        print("   • python dynamic_analysis.py - Análise completa")
        print("   • python daily_check.py - Check rápido diário")

    except Exception as e:
        print(f"❌ Erro na análise: {e}")

if __name__ == "__main__":
    main()
