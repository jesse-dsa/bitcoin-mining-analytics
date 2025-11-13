# scripts/database_schema_analyzer.py
import sys
import os
import duckdb

# Adicionar diretório pai ao path para imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from database.duckdb_manager import DuckDBManager
    DB_AVAILABLE = True
except ImportError as e:
    print(f"❌ Gerenciador de banco não disponível: {e}")
    DB_AVAILABLE = False

class DatabaseSchemaAnalyzer:
    """Analisador completo do schema do banco para mapeamento DER"""

    def __init__(self):
        if not DB_AVAILABLE:
            print("❌ Banco de dados não disponível.")
            sys.exit(1)

        self.db = DuckDBManager()
        self.conn = duckdb.connect("data/bitcoin_analytics.duckdb")

    def get_complete_schema(self):
        """Obtém o schema completo do banco"""
        schema = {}

        try:
            # Listar todas as tabelas
            tables = self.conn.execute("SHOW TABLES").fetchall()

            for table_info in tables:
                table_name = table_info[0]
                schema[table_name] = self._get_table_details(table_name)

            return schema

        except Exception as e:
            print(f"❌ Erro ao obter schema: {e}")
            return {}

    def _get_table_details(self, table_name):
        """Obtém detalhes completos de uma tabela"""
        details = {
            'columns': [],
            'primary_key': None,
            'foreign_keys': [],
            'indexes': [],
            'constraints': []
        }

        try:
            # Obter colunas
            columns = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            for col in columns:
                column_info = {
                    'name': col[0],
                    'type': col[1],
                    'nullable': col[2] == 'YES',
                    'default': col[3],
                    'pk': False,
                    'fk': False
                }
                details['columns'].append(column_info)

            # Tentar identificar chave primária (DuckDB não tem INFORMATION_SCHEMA completo)
            details['primary_key'] = self._infer_primary_key(table_name, details['columns'])

            # Identificar chaves estrangeiras
            details['foreign_keys'] = self._infer_foreign_keys(table_name, details['columns'])

            # Obter índices
            details['indexes'] = self._get_indexes(table_name)

            return details

        except Exception as e:
            print(f"❌ Erro ao obter detalhes da tabela {table_name}: {e}")
            return details

    def _infer_primary_key(self, table_name, columns):
        """Infere a chave primária baseado nas convenções"""
        # Verificar coluna 'id'
        for col in columns:
            if col['name'].lower() == 'id' and 'int' in col['type'].lower():
                return col['name']

        # Verificar coluna com sufixo _id
        for col in columns:
            if col['name'].lower().endswith('_id') and 'int' in col['type'].lower():
                return col['name']

        return None

    def _infer_foreign_keys(self, table_name, columns):
        """Infere chaves estrangeiras baseado nas convenções"""
        foreign_keys = []

        # Padrões comuns de FK
        fk_patterns = ['_id', '_fk', 'id_']

        for col in columns:
            col_name = col['name'].lower()

            # Verificar se parece uma FK
            is_fk_candidate = any(pattern in col_name for pattern in fk_patterns)
            is_fk_candidate = is_fk_candidate and col_name != 'id'  # Não é PK

            if is_fk_candidate and 'int' in col['type'].lower():
                # Tentar inferir a tabela referenciada
                referenced_table = self._infer_referenced_table(col_name)
                foreign_keys.append({
                    'column': col['name'],
                    'referenced_table': referenced_table,
                    'referenced_column': 'id'
                })

        return foreign_keys

    def _infer_referenced_table(self, column_name):
        """Infere a tabela referenciada por uma FK"""
        # Remover sufixos comuns
        table_name = column_name.replace('_id', '').replace('_fk', '').replace('id_', '')

        # Mapeamentos específicos do nosso schema
        mappings = {
            'network_metrics': 'bitcoin_network_metrics',
            'metrics': 'bitcoin_network_metrics'
        }

        return mappings.get(table_name, table_name)

    def _get_indexes(self, table_name):
        """Obtém índices da tabela"""
        try:
            # DuckDB não tem uma maneira direta de listar índices via SQL
            # Vamos assumir índices baseados em padrões comuns
            indexes = []

            # Índice comum em timestamp
            if any('timestamp' in col['name'].lower() for col in self._get_table_columns(table_name)):
                indexes.append({
                    'name': f'idx_{table_name}_timestamp',
                    'columns': ['timestamp'],
                    'type': 'BTREE'
                })

            # Índice comum em data_source
            if any('data_source' in col['name'].lower() for col in self._get_table_columns(table_name)):
                indexes.append({
                    'name': f'idx_{table_name}_source',
                    'columns': ['data_source'],
                    'type': 'BTREE'
                })

            return indexes

        except Exception as e:
            print(f"❌ Erro ao obter índices de {table_name}: {e}")
            return []

    def _get_table_columns(self, table_name):
        """Obtém colunas de uma tabela (versão corrigida)"""
        try:
            columns_info = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            # Converter para lista de dicionários para consistência
            columns = []
            for col in columns_info:
                columns.append({
                    'name': col[0],
                    'type': col[1],
                    'nullable': col[2] == 'YES'
                })
            return columns
        except Exception as e:
            print(f"❌ Erro ao obter colunas de {table_name}: {e}")
            return []

    def generate_der_documentation(self):
        """Gera documentação completa do DER"""
        schema = self.get_complete_schema()

        print("\n" + "="*100)
        print("🎯 DIAGRAMA ENTIDADE-RELACIONAMENTO (DER) - BITCOIN MINING ANALYTICS")
        print("="*100)

        # Resumo do banco
        self._print_database_summary(schema)

        # Detalhes de cada entidade
        for table_name, details in schema.items():
            self._print_entity_details(table_name, details)

        # Relacionamentos
        self._print_relationships(schema)

        # Script SQL de criação
        self._print_sql_creation_script(schema)

        # Legenda
        self._print_legend()

    def _print_database_summary(self, schema):
        """Imprime resumo do banco"""
        print(f"\n📊 RESUMO DO BANCO DE DADOS")
        print("-" * 50)
        print(f"🏠 Nome do Banco: bitcoin_analytics")
        print(f"📁 Total de Entidades: {len(schema)}")

        total_columns = sum(len(details['columns']) for details in schema.values())
        total_indexes = sum(len(details['indexes']) for details in schema.values())
        total_fks = sum(len(details['foreign_keys']) for details in schema.values())

        print(f"📋 Total de Atributos: {total_columns}")
        print(f"🔗 Total de Relacionamentos: {total_fks}")
        print(f"📈 Total de Índices: {total_indexes}")

        print(f"\n📋 LISTA DE ENTIDADES:")
        for table_name in schema.keys():
            print(f"   🗂️  {table_name}")

    def _print_entity_details(self, table_name, details):
        """Imprime detalhes de uma entidade"""
        print(f"\n{'='*80}")
        print(f"🏷️  ENTIDADE: {table_name.upper()}")
        print(f"{'='*80}")

        # Chave Primária
        if details['primary_key']:
            print(f"🔑 CHAVE PRIMÁRIA: {details['primary_key']}")
        else:
            print(f"⚠️  CHAVE PRIMÁRIA: Não identificada (inferir)")

        # Atributos
        print(f"\n📋 ATRIBUTOS:")
        print(f"   {'Nome':<25} {'Tipo':<15} {'Nulo?':<8} {'Default':<15} {'Chave':<10}")
        print(f"   {'-'*25} {'-'*15} {'-'*8} {'-'*15} {'-'*10}")

        for col in details['columns']:
            nullable = 'SIM' if col['nullable'] else 'NÃO'
            default = str(col['default']) if col['default'] else 'NULL'

            # Identificar tipo de chave
            key_type = ''
            if col['name'] == details['primary_key']:
                key_type = '🔑 PK'
            elif any(fk['column'] == col['name'] for fk in details['foreign_keys']):
                key_type = '🔗 FK'

            print(f"   {col['name']:<25} {col['type']:<15} {nullable:<8} {default:<15} {key_type:<10}")

        # Chaves Estrangeiras
        if details['foreign_keys']:
            print(f"\n🔗 CHAVES ESTRANGEIRAS:")
            for fk in details['foreign_keys']:
                print(f"   {fk['column']} → {fk['referenced_table']}({fk['referenced_column']})")

        # Índices
        if details['indexes']:
            print(f"\n📈 ÍNDICES:")
            for idx in details['indexes']:
                columns_str = ', '.join(idx['columns'])
                print(f"   {idx['name']} ({columns_str}) - {idx['type']}")

    def _print_relationships(self, schema):
        """Imprime os relacionamentos entre entidades"""
        print(f"\n{'='*80}")
        print(f"🔗 RELACIONAMENTOS ENTRE ENTIDADES")
        print(f"{'='*80}")

        relationships = []

        for table_name, details in schema.items():
            for fk in details['foreign_keys']:
                relationships.append({
                    'from_table': table_name,
                    'from_column': fk['column'],
                    'to_table': fk['referenced_table'],
                    'to_column': fk['referenced_column'],
                    'type': '1:N'  # Assumindo muitos-para-um
                })

        if relationships:
            for rel in relationships:
                print(f"   {rel['from_table']}.{rel['from_column']} → {rel['to_table']}.{rel['to_column']} ({rel['type']})")
        else:
            print("   ℹ️  Nenhum relacionamento explícito identificado")

        # Relacionamentos implícitos
        print(f"\n💡 RELACIONAMENTOS IMPLÍCITOS (por convenção):")
        if 'profitability_analysis' in schema and 'bitcoin_network_metrics' in schema:
            print("   💰 profitability_analysis → bitcoin_network_metrics (1:1 via network_metrics_id)")
        if 'mining_metrics' in schema and 'bitcoin_network_metrics' in schema:
            print("   ⛏️  mining_metrics → bitcoin_network_metrics (1:1 via network_metrics_id)")

    def _print_sql_creation_script(self, schema):
        """Imprime script SQL de criação das tabelas"""
        print(f"\n{'='*80}")
        print(f"🐘 SCRIPT SQL DE CRIAÇÃO (PostgreSQL-style)")
        print(f"{'='*80}")

        for table_name, details in schema.items():
            print(f"\n-- Tabela: {table_name}")
            print(f"CREATE TABLE {table_name} (")

            # Colunas
            columns_sql = []
            for col in details['columns']:
                nullable = "" if not col['nullable'] else " NULL"
                default = f" DEFAULT {col['default']}" if col['default'] else ""

                # Adicionar constraint de PK
                pk_constraint = " PRIMARY KEY" if col['name'] == details['primary_key'] else ""

                col_sql = f"    {col['name']} {col['type']}{nullable}{default}{pk_constraint}"
                columns_sql.append(col_sql)

            # Adicionar FKs
            for fk in details['foreign_keys']:
                fk_sql = f"    FOREIGN KEY ({fk['column']}) REFERENCES {fk['referenced_table']}({fk['referenced_column']})"
                columns_sql.append(fk_sql)

            print(",\n".join(columns_sql))
            print(");")

            # Índices
            for idx in details['indexes']:
                columns_str = ', '.join(idx['columns'])
                print(f"CREATE INDEX {idx['name']} ON {table_name} ({columns_str});")

    def _print_legend(self):
        """Imprime legenda do DER"""
        print(f"\n{'='*80}")
        print(f"📖 LEGENDA DO DIAGRAMA DER")
        print(f"{'='*80}")

        legend_items = [
            ("🔑", "Chave Primária (PK)", "Identificador único da entidade"),
            ("🔗", "Chave Estrangeira (FK)", "Relacionamento com outra entidade"),
            ("🗂️", "Entidade/Tabela", "Conjunto de dados relacionados"),
            ("📋", "Atributo/Coluna", "Campo de dados da entidade"),
            ("📈", "Índice", "Melhora performance de consultas"),
            ("→", "Relacionamento", "Conexão entre entidades"),
            ("1:N", "Cardinalidade", "Um-para-Muitos"),
            ("1:1", "Cardinalidade", "Um-para-Um")
        ]

        for symbol, term, description in legend_items:
            print(f"   {symbol} {term:<20} - {description}")

    def generate_plantuml_script(self):
        """Gera script PlantUML para diagrama automático"""
        schema = self.get_complete_schema()

        print(f"\n{'='*80}")
        print(f"🌿 SCRIPT PLANTUML PARA DIAGRAMA DER")
        print(f"{'='*80}")

        plantuml_script = """
@startuml BitcoinMiningAnalytics_DER

skinparam groupInheritance 2
skinparam linetype ortho
skinparam nodesep 50
skinparam ranksep 50

' Definir entidades
"""

        # Entidades
        for table_name, details in schema.items():
            plantuml_script += f'\nentity "{table_name.upper()}" {{\n'

            # Chave primária
            if details['primary_key']:
                plantuml_script += f"  **{details['primary_key']}** : {self._get_plantuml_type(details['primary_key'], details)}\n"

            # Demais atributos
            for col in details['columns']:
                if col['name'] != details['primary_key']:
                    is_fk = any(fk['column'] == col['name'] for fk in details['foreign_keys'])
                    marker = "" if not is_fk else " [FK]"
                    plantuml_script += f"  {col['name']} : {self._get_plantuml_type(col['name'], details)}{marker}\n"

            plantuml_script += "}\n"

        # Relacionamentos
        plantuml_script += "\n' Relacionamentos\n"
        for table_name, details in schema.items():
            for fk in details['foreign_keys']:
                plantuml_script += f'{table_name.upper()} ||--o| {fk["referenced_table"].upper()} : "{fk["column"]}"\n'

        plantuml_script += "\n@enduml"

        print(plantuml_script)

        # Salvar em arquivo
        with open("database_der_plantuml.txt", "w", encoding="utf-8") as f:
            f.write(plantuml_script)

        print(f"\n💾 Script PlantUML salvo em: database_der_plantuml.txt")
        print("🌐 Use em: https://www.plantuml.com/plantuml/uml/")

    def _get_plantuml_type(self, column_name, details):
        """Converte tipo SQL para formato PlantUML"""
        for col in details['columns']:
            if col['name'] == column_name:
                sql_type = col['type'].upper()

                if 'INT' in sql_type:
                    return 'INTEGER'
                elif 'DOUBLE' in sql_type or 'FLOAT' in sql_type:
                    return 'FLOAT'
                elif 'VARCHAR' in sql_type or 'TEXT' in sql_type:
                    return 'STRING'
                elif 'TIMESTAMP' in sql_type or 'DATE' in sql_type:
                    return 'TIMESTAMP'
                elif 'BOOL' in sql_type:
                    return 'BOOLEAN'
                else:
                    return sql_type

        return 'STRING'

def main():
    """Função principal"""
    print("🚀 ANALISADOR DE SCHEMA - DIAGRAMA ENTIDADE-RELACIONAMENTO")

    if not DB_AVAILABLE:
        print("❌ Banco de dados não disponível.")
        print("   Execute primeiro: python scripts/bitcoin_blockchair_dashboard.py")
        return

    try:
        analyzer = DatabaseSchemaAnalyzer()

        # Gerar documentação completa do DER
        analyzer.generate_der_documentation()

        # Gerar script PlantUML
        analyzer.generate_plantuml_script()

        print(f"\n🎯 PRÓXIMOS PASSOS:")
        print("   1. 📊 Diagrama visual: Copie o script PlantUML para https://www.plantuml.com/")
        print("   2. 📝 Documentação: Use o DER gerado para documentação do sistema")
        print("   3. 🔍 Análise: Verifique se os relacionamentos estão corretos")
        print("   4. 🗃️  Modelagem: Use como base para evolução do banco")

    except Exception as e:
        print(f"❌ Erro no analisador: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'analyzer' in locals():
            analyzer.conn.close()

if __name__ == "__main__":
    main()
