# scripts/test_database_fix.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.duckdb_manager import DuckDBManager

def test_database_fix():
    """Testa se as correções do banco funcionam"""
    print("🧪 TESTANDO CORREÇÕES DO DUCKDB")
    print("=" * 50)

    try:
        # Testar inicialização
        db = DuckDBManager()
        print("✅ DuckDBManager inicializado com sucesso")

        # Testar informações do banco
        info = db.get_database_info()
        print(f"✅ Informações do banco obtidas:")
        print(f"   📁 Arquivo: {info['database_path']}")
        print(f"   💾 Tamanho: {info['database_size_mb']} MB")

        # Testar inserção de dados de exemplo
        sample_data = {
            'blocks': 840000,
            'transactions': 900000000,
            'outputs': 2000000000,
            'circulation': 19500000,
            'blocks_24h': 144,
            'transactions_24h': 300000,
            'volume_24h': 50000000000,
            'hashrate_24h': 600000000000000000000,
            'difficulty': 80000000000000,
            'mempool_transactions': 5000,
            'mempool_size': 10000000,
            'mempool_tps': 5.5,
            'average_transaction_fee_24h': 15000,
            'median_transaction_fee_24h': 12000,
            'average_transaction_fee_usd_24h': 15.0,
            'suggested_transaction_fee_per_byte_sat': 20,
            'market_price_usd': 65000.0,
            'market_cap_usd': 1300000000000,
            'market_dominance_percentage': 52.5,
            'blockchain_size': 500000000000,
            'nodes': 15000,
            'hodling_addresses': 50000000,
            'next_difficulty_estimate': 85000000000000
        }

        network_id = db.save_network_metrics(sample_data, "test_source")

        if network_id:
            print(f"✅ Dados de teste salvos com sucesso - ID: {network_id}")

            # Testar recuperação de dados
            latest_id = db.get_latest_network_metrics_id()
            print(f"✅ Último ID obtido: {latest_id}")

            # Testar recuperação de métricas
            metrics = db.get_latest_metrics(3)
            print(f"✅ {len(metrics)} métricas recuperadas")

            for metric in metrics:
                print(f"   📊 ID:{metric['id']} | {metric['timestamp']} | ${metric['price_usd']:,.0f} | {metric['hashrate_ehs']:,.1f} EH/s")

            # Testar salvamento de análise de lucratividade
            profitability_data = {
                'miner_model': 'S19 XP',
                'miner_hashrate_th': 140,
                'miner_power_w': 3010,
                'energy_cost_per_kwh': 0.08,
                'daily_revenue_usd': 15.50,
                'daily_energy_cost_usd': 5.78,
                'daily_profit_usd': 9.72,
                'profit_margin_percentage': 62.7,
                'roi_days': 450,
                'break_even_days': 420,
                'monthly_profit_usd': 291.6,
                'annual_profit_usd': 3547.8
            }

            success = db.save_profitability_analysis(profitability_data, network_id)
            if success:
                print("✅ Análise de lucratividade salva com sucesso")
            else:
                print("❌ Falha ao salvar análise de lucratividade")

        else:
            print("❌ Falha ao salvar dados de teste")

        return True

    except Exception as e:
        print(f"❌ Erro no teste: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_database_fix()

    if success:
        print("\n🎉 TODOS OS TESTES PASSARAM! O banco está funcionando corretamente.")
    else:
        print("\n💥 ALGUNS TESTES FALHARAM. Verifique os logs acima.")
