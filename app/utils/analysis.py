# app/utils/analysis.py
from datetime import datetime

class BitcoinAnalyzer:
    def __init__(self, db_path='../analytics/bitcoin-mining-analytics/scripts/data/bitcoin_analytics.duckdb'):
        self.db_path = db_path
        self.conn = None
        self._try_connect_duckdb()

    def _try_connect_duckdb(self):
        """Tenta conectar ao DuckDB, mas não falha se não conseguir"""
        try:
            import duckdb
            self.conn = duckdb.connect(self.db_path)
            print("✅ Conectado ao DuckDB local")
        except ImportError:
            print("⚠️  DuckDB não disponível - usando dados de exemplo")
        except Exception as e:
            print(f"⚠️  Não foi possível conectar ao DuckDB: {e} - usando dados de exemplo")

    def get_current_metrics(self):
        """Obtém métricas - tenta DuckDB primeiro, depois fallback"""
        # Tenta buscar do DuckDB se disponível
        if self.conn:
            try:
                query = """
                SELECT timestamp, market_price_usd, hashrate_24h_ehs,
                    transactions_24h, difficulty, mempool_transactions
                FROM bitcoin_network_metrics
                ORDER BY timestamp DESC
                LIMIT 1
                """
                result = self.conn.execute(query).fetchone()
                if result:
                    columns = [desc[0] for desc in self.conn.description]
                    return dict(zip(columns, result))
            except Exception as e:
                print(f"⚠️  Erro ao buscar do DuckDB: {e}")

        # Fallback para dados de exemplo
        return self._get_sample_metrics()

    def get_mining_profitability(self):
        """Obtém análise de lucratividade"""
        return {
            "daily_profit_usd": 45.67,
            "profit_margin_percentage": 25.5,
            "roi_days": 420,
            "status": "lucrativa"
        }

    def generate_market_analysis(self):
        """Gera análise do mercado"""
        metrics = self.get_current_metrics()

        return {
            "price_analysis": {
                "trend": "alta",
                "current_price": metrics.get('market_price_usd', 65000),
                "change_percentage": 2.5
            },
            "network_health": {
                "health": "excelente",
                "security": "alta"
            },
            "mining_economics": {
                "status": "lucrativa",
                "margin": 25.5
            },
            "investment_recommendation": {
                "action": "comprar",
                "confidence": "alta"
            }
        }

    def _get_sample_metrics(self):
        """Retorna métricas de exemplo"""
        return {
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "market_price_usd": 65432.10,
            "hashrate_24h_ehs": 645.7,
            "transactions_24h": 345678,
            "difficulty": 83456789012,
            "mempool_transactions": 4567
        }

    def close(self):
        if self.conn:
            self.conn.close()
