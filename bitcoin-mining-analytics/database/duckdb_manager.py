# database/duckdb_manager.py
import duckdb
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
import pandas as pd

class DuckDBManager:
    """
    Gerenciador de banco de dados DuckDB para Bitcoin Mining Analytics
    Versão FINAL - sem mismatch de parâmetros
    """

    def __init__(self, db_path: str = "data/bitcoin_analytics.duckdb"):
        self.db_path = db_path
        self.setup_logging()
        self._init_database()

    def setup_logging(self):
        """Configura logging para o gerenciador de banco"""
        self.logger = logging.getLogger(__name__)

    def _init_database(self):
        """Inicializa o banco DuckDB e cria tabelas se não existirem"""
        try:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

            conn = duckdb.connect(self.db_path)

            # ✅ CORREÇÃO: Schema simplificado e compatível
            conn.execute("""
            CREATE TABLE IF NOT EXISTS bitcoin_network_metrics (
                id BIGINT PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_source VARCHAR,

                -- Métricas principais (reduzidas para evitar complexidade)
                blocks_24h INTEGER,
                transactions_24h INTEGER,
                difficulty DOUBLE,
                hashrate_24h_ehs DOUBLE,
                market_price_usd DOUBLE,

                -- Mempool e taxas
                mempool_transactions INTEGER,
                average_transaction_fee_usd_24h DOUBLE,

                -- Blockchain
                nodes INTEGER,
                blockchain_size BIGINT,

                -- Dados completos em JSON
                raw_data JSON
            )
            """)

            # Tabela de análise de lucratividade (simplificada)
            conn.execute("""
            CREATE TABLE IF NOT EXISTS profitability_analysis (
                id BIGINT PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                network_metrics_id BIGINT,
                miner_model VARCHAR,
                daily_revenue_usd DOUBLE,
                daily_energy_cost_usd DOUBLE,
                daily_profit_usd DOUBLE,
                profit_margin_percentage DOUBLE,
                roi_days DOUBLE,
                status VARCHAR
            )
            """)

            # Tabela de snapshots
            conn.execute("""
            CREATE TABLE IF NOT EXISTS bitcoin_snapshots (
                id BIGINT PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                snapshot_type VARCHAR,
                data_source VARCHAR,
                snapshot_data JSON
            )
            """)

            # Índices para performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON bitcoin_network_metrics(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_metrics_source ON bitcoin_network_metrics(data_source)")

            conn.close()
            self.logger.info("✅ Banco de dados DuckDB inicializado com sucesso")

        except Exception as e:
            self.logger.error(f"❌ Erro ao inicializar banco de dados: {e}")
            raise

    def _get_connection(self):
        """Estabelece conexão com o DuckDB"""
        return duckdb.connect(self.db_path)

    def _get_next_id(self, table_name: str) -> int:
        """Obtém o próximo ID para uma tabela"""
        try:
            conn = self._get_connection()
            result = conn.execute(f"SELECT COALESCE(MAX(id), 0) FROM {table_name}").fetchone()
            conn.close()
            return result[0] + 1 if result else 1
        except Exception as e:
            self.logger.error(f"❌ Erro ao obter próximo ID para {table_name}: {e}")
            return 1

    def save_network_metrics(self, data: Dict, source: str = "blockchair") -> int:
        """
        Salva métricas da rede Bitcoin no banco de dados
        ✅ CORREÇÃO: Schema simplificado para evitar mismatch
        """
        try:
            conn = self._get_connection()

            # ✅ CORREÇÃO: Obter próximo ID manualmente
            next_id = self._get_next_id('bitcoin_network_metrics')

            # ✅ CORREÇÃO: Converter tudo para float de forma segura
            def safe_float(value, default=0.0):
                try:
                    return float(value) if value is not None else default
                except (ValueError, TypeError):
                    return default

            # ✅ CORREÇÃO: Calcular hashrate em EH/s
            hashrate_hs = safe_float(data.get('hashrate_24h', 0))
            hashrate_ehs = hashrate_hs / 1e18

            # ✅ CORREÇÃO: Query SIMPLIFICADA com contagem exata de parâmetros
            query = """
            INSERT INTO bitcoin_network_metrics (
                id, timestamp, data_source, blocks_24h, transactions_24h,
                difficulty, hashrate_24h_ehs, market_price_usd,
                mempool_transactions, average_transaction_fee_usd_24h,
                nodes, blockchain_size, raw_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            # ✅ CORREÇÃO: Valores EXATAMENTE na mesma ordem e quantidade
            values = (
                next_id,                                    # id
                datetime.now(),                            # timestamp
                source,                                    # data_source
                data.get('blocks_24h', 0),                 # blocks_24h
                data.get('transactions_24h', 0),           # transactions_24h
                safe_float(data.get('difficulty', 0)),     # difficulty
                hashrate_ehs,                              # hashrate_24h_ehs
                safe_float(data.get('market_price_usd', 0)), # market_price_usd
                data.get('mempool_transactions', 0),       # mempool_transactions
                safe_float(data.get('average_transaction_fee_usd_24h', 0)), # avg_fee_usd
                data.get('nodes', 0),                      # nodes
                data.get('blockchain_size', 0),            # blockchain_size
                json.dumps(data, default=str)              # raw_data
            )

            # ✅ CORREÇÃO: Verificar contagem de parâmetros
            expected_params = 13
            actual_params = len(values)

            if expected_params != actual_params:
                self.logger.error(f"❌ Mismatch de parâmetros: esperado {expected_params}, obtido {actual_params}")
                return False

            conn.execute(query, values)
            conn.close()

            self.logger.info(f"✅ Métricas de rede salvas - ID: {next_id}, Fonte: {source}")
            return next_id

        except Exception as e:
            self.logger.error(f"❌ Erro ao salvar métricas de rede: {e}")
            return False

    def save_profitability_analysis(self, profitability: Dict, network_metrics_id: int) -> bool:
        """Salva análise de lucratividade"""
        try:
            conn = self._get_connection()

            # ✅ CORREÇÃO: Obter próximo ID manualmente
            next_id = self._get_next_id('profitability_analysis')

            # ✅ CORREÇÃO: Tipagem segura
            def safe_float(value, default=0.0):
                try:
                    return float(value) if value is not None else default
                except (ValueError, TypeError):
                    return default

            daily_profit = safe_float(profitability.get('daily_profit_usd', 0))
            status = "PROFIT" if daily_profit > 0 else "LOSS"

            # ✅ CORREÇÃO: Query simplificada
            query = """
            INSERT INTO profitability_analysis (
                id, timestamp, network_metrics_id, miner_model,
                daily_revenue_usd, daily_energy_cost_usd, daily_profit_usd,
                profit_margin_percentage, roi_days, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            values = (
                next_id,                                    # id
                datetime.now(),                            # timestamp
                network_metrics_id,                        # network_metrics_id
                profitability.get('miner_model', 'S19 XP'), # miner_model
                safe_float(profitability.get('daily_revenue_usd', 0)), # daily_revenue_usd
                safe_float(profitability.get('daily_energy_cost_usd', 0)), # daily_energy_cost_usd
                daily_profit,                              # daily_profit_usd
                safe_float(profitability.get('profit_margin_percentage', 0)), # profit_margin_percentage
                safe_float(profitability.get('roi_days', 0)), # roi_days
                status                                     # status
            )

            conn.execute(query, values)
            conn.close()

            self.logger.info(f"✅ Análise de lucratividade salva - ID: {next_id}")
            return True

        except Exception as e:
            self.logger.error(f"❌ Erro ao salvar análise de lucratividade: {e}")
            return False

    def save_comprehensive_snapshot(self, comprehensive_data: Dict, snapshot_type: str = "full") -> bool:
        """Salva snapshot completo"""
        try:
            conn = self._get_connection()

            # ✅ CORREÇÃO: Obter próximo ID manualmente
            next_id = self._get_next_id('bitcoin_snapshots')

            source = comprehensive_data.get('metadata', {}).get('success_sources', ['unknown'])[0]

            # ✅ CORREÇÃO: Query simplificada
            query = """
            INSERT INTO bitcoin_snapshots (
                id, timestamp, snapshot_type, data_source, snapshot_data
            ) VALUES (?, ?, ?, ?, ?)
            """

            values = (
                next_id,                                    # id
                datetime.now(),                            # timestamp
                snapshot_type,                             # snapshot_type
                source,                                    # data_source
                json.dumps(comprehensive_data, default=str) # snapshot_data
            )

            conn.execute(query, values)
            conn.close()

            self.logger.info(f"✅ Snapshot {snapshot_type} salvo - ID: {next_id}")
            return True

        except Exception as e:
            self.logger.error(f"❌ Erro ao salvar snapshot: {e}")
            return False

    def get_latest_network_metrics_id(self) -> Optional[int]:
        """Obtém o ID da métrica de rede mais recente"""
        try:
            conn = self._get_connection()
            result = conn.execute("SELECT MAX(id) FROM bitcoin_network_metrics").fetchone()
            conn.close()
            return result[0] if result and result[0] is not None else None
        except Exception as e:
            self.logger.error(f"❌ Erro ao obter último ID: {e}")
            return None

    def get_latest_metrics(self, limit: int = 10) -> List[Dict]:
        """Recupera as métricas mais recentes"""
        try:
            conn = self._get_connection()

            query = """
            SELECT
                id, timestamp, data_source, blocks_24h, transactions_24h,
                hashrate_24h_ehs, difficulty, market_price_usd,
                mempool_transactions, average_transaction_fee_usd_24h
            FROM bitcoin_network_metrics
            ORDER BY timestamp DESC
            LIMIT ?
            """

            result = conn.execute(query, (limit,)).fetchall()
            conn.close()

            metrics = []
            for row in result:
                metrics.append({
                    'id': row[0],
                    'timestamp': row[1],
                    'source': row[2],
                    'blocks_24h': row[3],
                    'transactions_24h': row[4],
                    'hashrate_ehs': row[5],
                    'difficulty': row[6],
                    'price_usd': row[7],
                    'mempool_txs': row[8],
                    'avg_fee_usd': row[9]
                })

            self.logger.info(f"✅ {len(metrics)} métricas recuperadas")
            return metrics

        except Exception as e:
            self.logger.error(f"❌ Erro ao recuperar métricas: {e}")
            return []

    def get_database_info(self) -> Dict[str, Any]:
        """Retorna informações sobre o banco de dados"""
        try:
            conn = self._get_connection()

            tables = ['bitcoin_network_metrics', 'profitability_analysis', 'bitcoin_snapshots']
            counts = {}

            for table in tables:
                result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                counts[table] = result[0] if result else 0

            # Verificar se há dados na tabela principal
            if counts['bitcoin_network_metrics'] > 0:
                oldest = conn.execute("SELECT MIN(timestamp) FROM bitcoin_network_metrics").fetchone()[0]
                newest = conn.execute("SELECT MAX(timestamp) FROM bitcoin_network_metrics").fetchone()[0]
            else:
                oldest = newest = "Nenhum dado"

            db_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0

            conn.close()

            info = {
                'database_path': self.db_path,
                'database_size_mb': round(db_size / (1024 * 1024), 2),
                'record_counts': counts,
                'oldest_record': oldest,
                'newest_record': newest,
                'total_records': sum(counts.values())
            }

            return info

        except Exception as e:
            self.logger.error(f"❌ Erro ao obter informações do banco: {e}")
            return {}

    def export_to_dataframe(self, table_name: str, limit: int = None) -> pd.DataFrame:
        """Exporta tabela para DataFrame pandas"""
        try:
            conn = self._get_connection()

            query = f"SELECT * FROM {table_name}"
            if limit:
                query += f" LIMIT {limit}"

            df = conn.execute(query).df()
            conn.close()

            self.logger.info(f"✅ Tabela {table_name} exportada para DataFrame: {len(df)} registros")
            return df

        except Exception as e:
            self.logger.error(f"❌ Erro ao exportar {table_name}: {e}")
            return pd.DataFrame()

    def clear_database(self):
        """Limpa todas as tabelas do banco (apenas para desenvolvimento)"""
        try:
            conn = self._get_connection()

            tables = ['bitcoin_snapshots', 'profitability_analysis', 'bitcoin_network_metrics']

            for table in tables:
                conn.execute(f"DELETE FROM {table}")

            conn.close()
            self.logger.info("✅ Banco de dados limpo")

        except Exception as e:
            self.logger.error(f"❌ Erro ao limpar banco: {e}")

def get_db_manager() -> DuckDBManager:
    """Retorna instância do gerenciador de banco de dados"""
    return DuckDBManager()

def test_database():
    """Função de teste para o gerenciador de banco"""
    print("🧪 Testando DuckDBManager...")

    try:
        db = DuckDBManager()

        info = db.get_database_info()
        print(f"✅ Banco inicializado: {info['database_path']}")
        print(f"📊 Total de registros: {info['total_records']}")
        print(f"📁 Tamanho: {info['database_size_mb']} MB")

        # Mostrar contagem por tabela
        for table, count in info['record_counts'].items():
            print(f"   📋 {table}: {count} registros")

        return db

    except Exception as e:
        print(f"❌ Erro no teste do banco: {e}")
        return None

if __name__ == "__main__":
    test_database()
