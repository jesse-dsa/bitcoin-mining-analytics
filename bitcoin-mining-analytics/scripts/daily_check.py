# scripts/daily_check.py
import sys
import os
from datetime import datetime  # ✅ CORREÇÃO: Importar datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from database.duckdb_manager import DuckDBManager
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False

def daily_portfolio_check():
    """Verificação rápida diária do seu portfolio"""
    if not DB_AVAILABLE:
        print("❌ Banco de dados não disponível")
        return

    try:
        db = DuckDBManager()
        metrics = db.get_latest_metrics(1)

        if not metrics:
            print("❌ Não foi possível obter dados do Bitcoin")
            return

        price = metrics[0]['price_usd']
        hashrate = metrics[0]['hashrate_ehs']

        print(f"\n📅 {datetime.now().strftime('%Y-%m-%d %H:%M')} - CHECK DIÁRIO")
        print("=" * 50)
        print(f"💰 BITCOIN: ${price:,.2f}")
        print(f"⚡ HASH RATE: {hashrate:,.1f} EH/s")

        # Seus níveis críticos
        print(f"\n🎯 SEUS NÍVEIS:")
        print(f"   🎯 Alvo: $84,500 (+30%)")
        print(f"   🛡️  Stop: $55,250 (-15%)")
        print(f"   💰 Sua Entrada: $65,000")

        # Análise de situação
        print(f"\n📊 SITUAÇÃO ATUAL:")
        if price >= 84500:
            print("   🎯 ATINGIU ALVO! Considerar realização de lucros")
            profit = ((price - 65000) / 65000) * 100
            print(f"   💰 Lucro: +{profit:.1f}% desde sua entrada")
        elif price <= 55250:
            print("   🚨 ATINGIU STOP! Reavaliar estratégia")
            loss = ((65000 - price) / 65000) * 100
            print(f"   📉 Prejuízo: -{loss:.1f}% desde sua entrada")
        elif price > 70000:
            print("   📈 BTC em ALTA - acima de $70k")
            gain = ((price - 65000) / 65000) * 100
            print(f"   💹 Ganho: +{gain:.1f}% desde sua entrada")
        elif price > 65000:
            print("   📈 BTC em alta desde sua entrada")
            gain = ((price - 65000) / 65000) * 100
            print(f"   💹 Ganho: +{gain:.1f}%")
        elif price > 60000:
            print("   📉 BTC em correção leve - normal em bull market")
            loss = ((65000 - price) / 65000) * 100
            print(f"   🔄 Queda: -{loss:.1f}% desde sua entrada")
        else:
            print("   📉 BTC em correção significativa")
            loss = ((65000 - price) / 65000) * 100
            print(f"   🔄 Queda: -{loss:.1f}% desde sua entrada")

        # Recomendação baseada no preço
        print(f"\n💡 RECOMENDAÇÃO:")
        if price < 60000:
            print("   ✅ COMPRAR MAIS - Preço muito atrativo")
        elif price < 70000:
            print("   🟡 SEGURAR - Preço razoável, manter estratégia")
        else:
            print("   🔴 AGUARDAR - Preço elevado, evitar novas entradas")

        # Status da mineração
        try:
            conn = db._get_connection()
            mining_data = conn.execute("""
                SELECT daily_profit_usd, profit_margin_percentage, status
                FROM profitability_analysis
                ORDER BY timestamp DESC LIMIT 1
            """).fetchone()
            conn.close()

            if mining_data:
                profit, margin, status = mining_data
                print(f"\n⛏️  MINERAÇÃO:")
                print(f"   💰 Lucro/dia: ${profit:.2f}")
                print(f"   📊 Margem: {margin:.1f}%")
                print(f"   🎯 Status: {'✅ LUCRO' if status == 'PROFIT' else '❌ PREJUÍZO'}")

        except Exception as e:
            print(f"\n⛏️  Mineração: Dados não disponíveis")

        print(f"\n{'='*50}")
        print("💡 Execute 'python integrated_dashboard.py' para análise completa")

    except Exception as e:
        print(f"❌ Erro no check diário: {e}")

if __name__ == "__main__":
    daily_portfolio_check()
