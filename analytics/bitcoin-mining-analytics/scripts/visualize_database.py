from database.duckdb_manager import DuckDBManager

# Inicializar
db = DuckDBManager()

# Salvar dados (agora sem erro!)
success = db.save_network_metrics(your_data, "blockchair")

if success:
    # Obter ID da métrica salva
    latest_id = db.get_latest_network_metrics_id()

    # Salvar análise de lucratividade
    db.save_profitability_analysis(profit_data, latest_id)
