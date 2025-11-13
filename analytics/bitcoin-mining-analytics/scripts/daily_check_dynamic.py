# scripts/daily_check_dynamic.py
import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from database.duckdb_manager import DuckDBManager
    from portfolio_manager import PortfolioManager
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False

def dynamic_daily_check():
    """Check diário dinâmico para múltiplos portfolios - VERSÃO CORRIGIDA"""
    if not DB_AVAILABLE:
        print("❌ Módulos não disponíveis")
        return

    try:
        db = DuckDBManager()
        portfolio_manager = PortfolioManager()

        metrics = db.get_latest_metrics(1)
        if not metrics:
            print("❌ Não foi possível obter dados do Bitcoin")
            return

        price = metrics[0]['price_usd']
        hashrate = metrics[0]['hashrate_ehs']

        print(f"\n📅 {datetime.now().strftime('%Y-%m-%d %H:%M')} - CHECK DIÁRIO DINÂMICO")
        print("=" * 60)
        print(f"💰 BITCOIN: ${price:,.2f}")
        print(f"⚡ HASH RATE: {hashrate:,.1f} EH/s")

        # Verificar todos os portfolios - VERSÃO CORRIGIDA
        portfolios = portfolio_manager.list_portfolios()
        if portfolios:
            print(f"\n🎯 PORTFOLIOS ATIVOS ({len(portfolios)}):")
            for portfolio_id, portfolio_data in portfolios.items():
                # ✅ CORREÇÃO: Somar corretamente os valores das alocações
                total_value = 0
                allocations = portfolio_data.get('allocations', {})

                for asset, allocation_data in allocations.items():
                    if isinstance(allocation_data, dict) and 'amount' in allocation_data:
                        total_value += allocation_data['amount']
                    else:
                        # Fallback para formato antigo
                        total_value += float(allocation_data) if isinstance(allocation_data, (int, float)) else 0

                print(f"   👤 {portfolio_data['owner']} - {portfolio_data['name']}: ${total_value:,.2f}")
        else:
            print(f"\n💡 Nenhum portfolio encontrado.")
            print("   Execute: python portfolio_manager.py")

        # Análise de mercado
        print(f"\n📊 ANÁLISE DE MERCADO:")
        if price >= 84500:
            print("   🎯 ATINGIU ALVO! Considerar realização de lucros")
        elif price <= 55250:
            print("   🚨 ATINGIU STOP! Reavaliar estratégia")
        elif price > 65000:
            print("   📈 BTC em alta desde entrada base")
        else:
            print("   📉 BTC em correção - normal em bull market")

        print(f"\n💡 RECOMENDAÇÃO:")
        if price < 60000:
            print("   ✅ COMPRAR MAIS - Preço muito atrativo")
        elif price < 70000:
            print("   🟡 SEGURAR - Preço razoável")
        else:
            print("   🔴 AGUARDAR - Preço elevado")

    except Exception as e:
        print(f"❌ Erro no check diário: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    dynamic_daily_check()
