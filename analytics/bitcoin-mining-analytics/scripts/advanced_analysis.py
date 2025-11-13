# scripts/advanced_analysis.py
import sys
import os
import pandas as pd
from datetime import datetime, timedelta
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from database.duckdb_manager import DuckDBManager
    DB_AVAILABLE = True
except ImportError as e:
    print(f"❌ Gerenciador de banco não disponível: {e}")
    DB_AVAILABLE = False

class AdvancedBitcoinAnalysis:
    """Sistema avançado de análise explicativa de dados Bitcoin"""

    def __init__(self):
        if not DB_AVAILABLE:
            print("❌ Banco de dados não disponível.")
            sys.exit(1)

        self.db = DuckDBManager()
        self.setup_logging()

    def setup_logging(self):
        """Configura o sistema de logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def comprehensive_market_analysis(self):
        """Análise completa do mercado Bitcoin"""
        print("\n" + "="*80)
        print("🎯 ANÁLISE EXPLICATIVA DO MERCADO BITCOIN")
        print("="*80)

        try:
            # Obter dados recentes
            metrics = self.db.get_latest_metrics(10)
            if not metrics:
                print("❌ Dados insuficientes para análise")
                return

            current_data = metrics[0]
            historical_data = metrics

            self._analyze_price_trends(historical_data, current_data)
            self._analyze_network_health(historical_data, current_data)
            self._analyze_mining_economics(current_data)
            self._generate_investment_recommendations(current_data)
            self._risk_assessment(historical_data)

        except Exception as e:
            self.logger.error(f"❌ Erro na análise: {e}")

    def _analyze_price_trends(self, historical_data, current_data):
        """Análise de tendências de preço"""
        print(f"\n💰 ANÁLISE DE TENDÊNCIAS DE PREÇO")
        print("-" * 50)

        prices = [m['price_usd'] for m in historical_data if m['price_usd']]
        if len(prices) < 2:
            print("   ℹ️  Dados insuficientes para análise de tendência")
            return

        current_price = current_data['price_usd']
        avg_price = sum(prices) / len(prices)
        max_price = max(prices)
        min_price = min(prices)

        trend = "📈 ALTA" if current_price > avg_price else "📉 BAIXA"
        volatility = ((max_price - min_price) / avg_price) * 100

        print(f"   💵 Preço Atual: ${current_data['price_usd']:,.2f}")
        print(f"   📊 Média Recente: ${avg_price:,.2f}")
        print(f"   🎯 Tendência: {trend}")
        print(f"   📈 Volatilidade: {volatility:.1f}%")

        # Análise de suporte e resistência
        if current_price > avg_price:
            print(f"   🟢 SINAL: Preço acima da média - Momentum positivo")
            if current_price < max_price:
                print(f"   🎯 Resistência: ${max_price:,.0f}")
        else:
            print(f"   🟡 SINAL: Preço abaixo da média - Cautela")
            print(f"   🛡️  Suporte: ${min_price:,.0f}")

    def _analyze_network_health(self, historical_data, current_data):
        """Análise da saúde da rede"""
        print(f"\n🌐 ANÁLISE DA SAÚDE DA REDE")
        print("-" * 50)

        hashrates = [m['hashrate_ehs'] for m in historical_data if m['hashrate_ehs']]
        transactions = [m['transactions_24h'] for m in historical_data if m['transactions_24h']]

        if hashrates:
            avg_hashrate = sum(hashrates) / len(hashrates)
            hash_trend = "📈 FORTE" if current_data['hashrate_ehs'] > avg_hashrate else "📉 MODERADO"

            print(f"   ⚡ Hash Rate: {current_data['hashrate_ehs']:,.1f} EH/s")
            print(f"   📊 Tendência: {hash_trend}")
            print(f"   🛡️  Segurança: {'🔒 ALTA' if current_data['hashrate_ehs'] > 500 else '🔓 MÉDIA'}")

        if transactions:
            avg_txs = sum(transactions) / len(transactions)
            tx_trend = "📈 ATIVA" if current_data['transactions_24h'] > avg_txs else "📉 NORMAL"

            print(f"   📦 Transações/dia: {current_data['transactions_24h']:,}")
            print(f"   🔄 Atividade: {tx_trend}")
            print(f"   💸 Taxa Média: ${current_data.get('avg_fee_usd', 0):.4f}")

    def _analyze_mining_economics(self, current_data):
        """Análise da economia de mineração"""
        print(f"\n⛏️  ANÁLISE ECONÔMICA DA MINERAÇÃO")
        print("-" * 50)

        try:
            conn = self.db._get_connection()

            # Última análise de lucratividade
            profit_analysis = conn.execute("""
                SELECT * FROM profitability_analysis
                ORDER BY timestamp DESC LIMIT 1
            """).fetchone()

            if profit_analysis:
                columns = [desc[0] for desc in conn.description]
                profit_data = dict(zip(columns, profit_analysis))

                daily_profit = profit_data.get('daily_profit_usd', 0)
                margin = profit_data.get('profit_margin_percentage', 0)
                roi = profit_data.get('roi_days', 0)
                status = profit_data.get('status', 'UNKNOWN')

                print(f"   💰 Lucro/dia: ${daily_profit:.2f}")
                print(f"   📊 Margem: {margin:.1f}%")
                print(f"   🔄 ROI: {roi:.0f} dias")
                print(f"   🎯 Status: {'✅ LUCRO' if status == 'PROFIT' else '❌ PREJUÍZO'}")

                # Análise de viabilidade
                if margin > 50:
                    print(f"   🟢 VIABILIDADE: EXCELENTE - Margem acima de 50%")
                elif margin > 30:
                    print(f"   🟡 VIABILIDADE: BOA - Margem aceitável")
                else:
                    print(f"   🔴 VIABILIDADE: CRÍTICA - Margem muito baixa")

                # Recomendação de mineração
                if daily_profit > 10 and margin > 40:
                    print(f"   💡 RECOMENDAÇÃO: ✅ EXPANSÃO - Condições favoráveis")
                elif daily_profit > 5:
                    print(f"   💡 RECOMENDAÇÃO: 🟡 MANUTENÇÃO - Monitorar mercado")
                else:
                    print(f"   💡 RECOMENDAÇÃO: 🔴 CAUTELA - Reavaliar operação")

            conn.close()

        except Exception as e:
            print(f"   ❌ Erro na análise de mineração: {e}")

    def _generate_investment_recommendations(self, current_data):
        """Gera recomendações de investimento baseadas em dados"""
        print(f"\n💼 RECOMENDAÇÕES DE INVESTIMENTO")
        print("-" * 50)

        price = current_data['price_usd']
        hashrate = current_data['hashrate_ehs']

        # Score baseado em múltiplos fatores
        score = 0

        # Fator Preço
        if price < 60000:
            score += 3
            price_rec = "🟢 COMPRAR - Preço atrativo"
        elif price < 80000:
            score += 1
            price_rec = "🟡 ACUMULAR - Preço razoável"
        else:
            score -= 1
            price_rec = "🔴 AGUARDAR - Preço elevado"

        # Fator Rede
        if hashrate > 800:
            score += 2
            network_rec = "🟢 FORTE - Rede segura"
        elif hashrate > 500:
            score += 1
            network_rec = "🟡 ESTÁVEL - Rede normal"
        else:
            network_rec = "🔴 FRACA - Monitorar"

        print(f"   💵 Preço: {price_rec}")
        print(f"   🌐 Rede: {network_rec}")

        # Recomendação final
        if score >= 4:
            recommendation = "🎯 RECOMENDAÇÃO: COMPRAR AGORA"
            reasoning = "Preço atrativo + Rede forte = Oportunidade excelente"
        elif score >= 2:
            recommendation = "🎯 RECOMENDAÇÃO: ACUMULAR GRADUAL"
            reasoning = "Condições favoráveis para entrada gradual"
        else:
            recommendation = "🎯 RECOMENDAÇÃO: AGUARDAR"
            reasoning = "Melhor esperar por correção ou melhora na rede"

        print(f"   {recommendation}")
        print(f"   💡 Fundamentação: {reasoning}")

    def _risk_assessment(self, historical_data):
        """Avaliação de riscos baseada em dados históricos"""
        print(f"\n🚨 AVALIAÇÃO DE RISCOS")
        print("-" * 50)

        prices = [m['price_usd'] for m in historical_data if m['price_usd']]
        if len(prices) < 3:
            print("   ℹ️  Dados insuficientes para análise de risco")
            return

        current_price = prices[0]
        avg_price = sum(prices) / len(prices)
        max_drawdown = (max(prices) - min(prices)) / max(prices) * 100

        print(f"   📉 Máxima Queda Histórica: {max_drawdown:.1f}%")

        # Níveis de suporte críticos
        support_levels = [avg_price * 0.9, avg_price * 0.8, avg_price * 0.7]
        print(f"   🛡️  Níveis de Suporte: ${support_levels[0]:,.0f} | ${support_levels[1]:,.0f} | ${support_levels[2]:,.0f}")

        # Alertas de risco
        if max_drawdown > 30:
            print(f"   🔴 ALTO RISCO: Volatilidade histórica elevada")
        elif max_drawdown > 20:
            print(f"   🟡 RISCO MODERADO: Volatilidade esperada")
        else:
            print(f"   🟢 RISCO BAIXO: Mercado estável")

        print(f"   💡 SUGESTÃO: Use stops em ${support_levels[1]:,.0f} (-20%)")

    def portfolio_analysis(self, investments):
        """
        Análise específica para portfolio do usuário
        investments: dict com { 'BTC': amount, 'ETH': amount, ... }
        """
        print(f"\n💼 ANÁLISE DO SEU PORTFOLIO")
        print("-" * 50)

        try:
            current_price = self.db.get_latest_metrics(1)[0]['price_usd']

            total_value = 0
            print("   📊 COMPOSIÇÃO DO PORTFOLIO:")

            for asset, amount in investments.items():
                # Preços aproximados (em produção, buscar de API)
                asset_prices = {
                    'BTC': current_price,
                    'ETH': current_price * 0.05,  # Aproximação
                    'LINK': current_price * 0.0003  # Aproximação
                }

                asset_value = amount * asset_prices.get(asset, 0)
                total_value += asset_value
                allocation = (asset_value / sum(investments.values())) * 100 if sum(investments.values()) > 0 else 0

                print(f"      {asset}: ${asset_value:,.2f} ({allocation:.1f}%)")

            print(f"   💰 VALOR TOTAL: ${total_value:,.2f}")

            # Análise de diversificação
            if len(investments) >= 3:
                print(f"   🌈 DIVERSIFICAÇÃO: ✅ ADEQUADA")
            else:
                print(f"   🌈 DIVERSIFICAÇÃO: ⚠️  CONCENTRADA")

        except Exception as e:
            print(f"   ❌ Erro na análise do portfolio: {e}")

    def generate_trading_signals(self):
        """Gera sinais de trading baseados em análise técnica"""
        print(f"\n📡 SINAIS DE TRADING")
        print("-" * 50)

        try:
            metrics = self.db.get_latest_metrics(5)
            if len(metrics) < 3:
                print("   ℹ️  Dados insuficientes para sinais")
                return

            prices = [m['price_usd'] for m in metrics]
            current_price = prices[0]

            # Análise de momentum simples
            price_change = ((current_price - prices[-1]) / prices[-1]) * 100

            if price_change > 5:
                signal = "🟢 COMPRAR - Momentum positivo forte"
            elif price_change > 2:
                signal = "🟡 COMPRAR LEVE - Momentum positivo"
            elif price_change < -5:
                signal = "🔴 VENDER - Momentum negativo forte"
            elif price_change < -2:
                signal = "🟡 VENDER PARCIAL - Momentum negativo"
            else:
                signal = "⚪ MANTER - Mercado lateral"

            print(f"   📊 Variação Recente: {price_change:+.1f}%")
            print(f"   🎯 Sinal: {signal}")

            # Suportes e resistências
            support = min(prices) * 0.95
            resistance = max(prices) * 1.05

            print(f"   🛡️  Suporte: ${support:,.0f}")
            print(f"   🎯 Resistência: ${resistance:,.0f}")

        except Exception as e:
            print(f"   ❌ Erro nos sinais: {e}")

def main():
    """Função principal"""
    print("🚀 SISTEMA AVANÇADO DE ANÁLISE EXPLICATIVA")

    if not DB_AVAILABLE:
        print("❌ Banco de dados não disponível.")
        return

    try:
        analyzer = AdvancedBitcoinAnalysis()

        # Análise completa do mercado
        analyzer.comprehensive_market_analysis()

        # Análise do portfolio específico (ajuste com seus valores)
        user_portfolio = {
            'BTC': 500,   # $500 em Bitcoin
            'ETH': 250,   # $250 em Ethereum
            'LINK': 250   # $250 em Chainlink
        }
        analyzer.portfolio_analysis(user_portfolio)

        # Sinais de trading
        analyzer.generate_trading_signals()

        print(f"\n" + "="*80)
        print("📋 PRÓXIMOS PASSOS RECOMENDADOS:")
        print("   1. Monitorar suportes e resistências identificados")
        print("   2. Reavaliar mineração se margem cair abaixo de 40%")
        print("   3. Considerar rebalanceamento do portfolio mensalmente")
        print("   4. Executar análise diária para acompanhar tendências")
        print("="*80)

    except Exception as e:
        print(f"❌ Erro no sistema de análise: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
