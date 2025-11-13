# src/data/collectors/blockchain_collector.py
import aiohttp
import asyncio
import json
import logging
from typing import Dict, List, Optional, Any
import os
import yaml
from datetime import datetime
from dataclasses import dataclass
import hashlib

@dataclass
class DataSourceConfig:
    """Configuração individual para cada fonte de dados"""
    name: str
    base_url: str
    endpoints: Dict[str, str]
    rate_limit: int
    api_key_required: bool
    api_key: str = ""
    timeout: int = 30
    headers: Dict[str, str] = None

class BlockchainCollector:
    """
    Coletor modular profissional para dados de blockchain de múltiplas fontes
    """

    def __init__(self, config_path: str = "config/data_sources.yaml"):
        self.setup_logging()
        self.config_path = config_path
        self.data_sources = self.load_data_sources_config()
        self.session = None
        self.collected_data = {}

    def setup_logging(self):
        """Configura logging para o coletor"""
        self.logger = logging.getLogger(__name__)

    def load_data_sources_config(self) -> Dict[str, DataSourceConfig]:
        """Carrega e parseia configurações de data_sources.yaml"""
        try:
            if not os.path.exists(self.config_path):
                self.logger.warning(f"Arquivo de configuração não encontrado: {self.config_path}")
                self.logger.info("📝 Usando configurações padrão...")
                return self.get_default_config()

            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            data_sources = {}

            # Parse blockchain data sources
            for source_name, source_config in config.get('blockchain_data', {}).items():
                data_sources[source_name] = DataSourceConfig(
                    name=source_name,
                    base_url=source_config.get('base_url', ''),
                    endpoints=source_config.get('endpoints', {}),
                    rate_limit=source_config.get('rate_limit', 1),
                    api_key_required=source_config.get('api_key_required', False),
                    timeout=source_config.get('timeout', 30),
                    headers={'User-Agent': 'Bitcoin-Mining-Analytics/1.0'}
                )

            self.logger.info(f"✅ Configurações carregadas: {len(data_sources)} fontes")
            return data_sources

        except Exception as e:
            self.logger.error(f"❌ Erro ao carregar configurações: {e}")
            self.logger.info("📝 Usando configurações padrão devido ao erro...")
            return self.get_default_config()

    def get_default_config(self) -> Dict[str, DataSourceConfig]:
        """Retorna configurações padrão quando arquivo não existe ou há erro"""
        default_config = {}

        # Blockchair - Fonte principal
        default_config['blockchair'] = DataSourceConfig(
            name='blockchair',
            base_url='https://api.blockchair.com/bitcoin',
            endpoints={
                'stats': '/stats',
                'blocks': '/blocks',
                'transactions': '/transactions',
                'mempool': '/mempool'
            },
            rate_limit=10,
            api_key_required=False,
            timeout=30,
            headers={'User-Agent': 'Bitcoin-Mining-Analytics/1.0'}
        )

        # Mempool.space - Dados de taxas e mempool
        default_config['mempool_space'] = DataSourceConfig(
            name='mempool_space',
            base_url='https://mempool.space/api',
            endpoints={
                'stats': '/v1/blocks',
                'fees': '/v1/fees/recommended',
                'mempool': '/v1/mempool'
            },
            rate_limit=15,
            api_key_required=False,
            timeout=30,
            headers={'User-Agent': 'Bitcoin-Mining-Analytics/1.0'}
        )

        # Blockchain.com - Dados alternativos
        default_config['blockchain_com'] = DataSourceConfig(
            name='blockchain_com',
            base_url='https://api.blockchain.com/v3',
            endpoints={
                'stats': '/exchangerates',
                'mining': '/mining'
            },
            rate_limit=5,
            api_key_required=False,
            timeout=30,
            headers={'User-Agent': 'Bitcoin-Mining-Analytics/1.0'}
        )

        self.logger.info("✅ Configurações padrão carregadas para 3 fontes")
        return default_config

    async def __aenter__(self):
        """Context manager para sessão HTTP"""
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Fecha sessão HTTP"""
        if self.session:
            await self.session.close()

    async def make_api_request(self, source: DataSourceConfig, endpoint: str) -> Optional[Dict]:
        """
        Faz requisição HTTP para API com tratamento robusto de erros
        """
        try:
            url = f"{source.base_url}{endpoint}"
            self.logger.debug(f"🌐 Requisitando: {url}")

            async with self.session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=source.timeout),
                headers=source.headers
            ) as response:

                if response.status == 200:
                    content_type = response.headers.get('content-type', '')

                    if 'application/json' in content_type:
                        data = await response.json()
                    else:
                        text = await response.text()
                        data = json.loads(text)  # Tenta parsear como JSON

                    self.logger.info(f"✅ {source.name}: Dados coletados com sucesso")
                    return data
                else:
                    self.logger.warning(f"⚠️ {source.name}: Status {response.status}")
                    return None

        except asyncio.TimeoutError:
            self.logger.error(f"⏰ {source.name}: Timeout na requisição")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ {source.name}: Erro ao decodificar JSON: {e}")
            return None
        except Exception as e:
            self.logger.error(f"❌ {source.name}: Erro na requisição: {e}")
            return None

    async def fetch_blockchair_data(self) -> Optional[Dict]:
        """Coleta dados abrangentes da Blockchair"""
        source = self.data_sources.get('blockchair')
        if not source:
            self.logger.error("❌ Configuração Blockchair não encontrada")
            return None

        try:
            # Coletar estatísticas principais
            stats_data = await self.make_api_request(source, source.endpoints.get('stats', '/stats'))

            if stats_data and 'data' in stats_data:
                blockchair_data = stats_data['data']

                # Coletar dados adicionais se disponíveis
                try:
                    blocks_data = await self.make_api_request(source, source.endpoints.get('blocks', '/blocks?limit=1'))
                    if blocks_data:
                        blockchair_data['recent_blocks'] = blocks_data.get('data', [])
                except Exception as e:
                    self.logger.warning(f"⚠️ Erro ao coletar blocos recentes: {e}")

                return blockchair_data
            else:
                return None

        except Exception as e:
            self.logger.error(f"❌ Erro na coleta Blockchair: {e}")
            return None

    async def fetch_mempool_space_data(self) -> Optional[Dict]:
        """Coleta dados do Mempool.space"""
        source = self.data_sources.get('mempool_space')
        if not source:
            self.logger.error("❌ Configuração Mempool.space não encontrada")
            return None

        try:
            # Coletar taxas recomendadas
            fees_data = await self.make_api_request(source, source.endpoints.get('fees', '/v1/fees/recommended'))

            # Coletar estatísticas de blocos
            blocks_data = await self.make_api_request(source, source.endpoints.get('stats', '/v1/blocks'))

            mempool_data = {}

            if fees_data:
                mempool_data['recommended_fees'] = fees_data

            if blocks_data:
                mempool_data['recent_blocks'] = blocks_data[:5] if isinstance(blocks_data, list) else blocks_data

            return mempool_data if mempool_data else None

        except Exception as e:
            self.logger.error(f"❌ Erro na coleta Mempool.space: {e}")
            return None

    async def fetch_blockchain_com_data(self) -> Optional[Dict]:
        """Coleta dados do Blockchain.com"""
        source = self.data_sources.get('blockchain_com')
        if not source:
            self.logger.warning("⚠️ Configuração Blockchain.com não encontrada")
            return None

        try:
            # Coletar estatísticas de mineração
            mining_data = await self.make_api_request(source, source.endpoints.get('mining', '/mining'))

            # Coletar taxas de câmbio
            stats_data = await self.make_api_request(source, source.endpoints.get('stats', '/exchangerates'))

            blockchain_com_data = {}

            if mining_data:
                blockchain_com_data['mining_stats'] = mining_data

            if stats_data:
                blockchain_com_data['exchange_rates'] = stats_data

            return blockchain_com_data if blockchain_com_data else None

        except Exception as e:
            self.logger.warning(f"⚠️ Erro na coleta Blockchain.com: {e}")
            return None

    def validate_data_quality(self, data: Dict, source: str) -> Dict:
        """
        Valida qualidade básica dos dados coletados
        """
        validation_result = {
            'source': source,
            'timestamp': datetime.now().isoformat(),
            'is_valid': False,
            'fields_count': 0,
            'missing_critical_fields': [],
            'warnings': []
        }

        if not data:
            validation_result['warnings'].append('Dados vazios ou nulos')
            return validation_result

        validation_result['fields_count'] = len(data)

        # Campos críticos esperados para Blockchair
        if source == 'blockchair':
            critical_fields = ['blocks', 'transactions', 'difficulty', 'hashrate_24h']
            missing_fields = [field for field in critical_fields if field not in data]

            if missing_fields:
                validation_result['missing_critical_fields'] = missing_fields
                validation_result['warnings'].append(f'Campos críticos faltando: {missing_fields}')
            else:
                validation_result['is_valid'] = True

        # Validações comuns para todas as fontes
        if isinstance(data, dict) and len(data) < 3:
            validation_result['warnings'].append('Poucos campos coletados')

        return validation_result

    def generate_data_hash(self, data: Dict) -> str:
        """Gera hash para verificação de integridade dos dados"""
        data_str = json.dumps(data, sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()

    async def collect_from_all_sources(self) -> Dict:
        """
        Coleta dados de todas as fontes configuradas de forma assíncrona
        """
        self.logger.info("🔄 Iniciando coleta abrangente de dados...")

        collection_start = datetime.now()
        tasks = {
            'blockchair': self.fetch_blockchair_data(),
            'mempool_space': self.fetch_mempool_space_data(),
            'blockchain_com': self.fetch_blockchain_com_data(),
        }

        # Executar todas as tarefas em paralelo
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        # Processar resultados
        collected_data = {}
        success_sources = []
        failed_sources = []

        for source_name, result in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                self.logger.error(f"❌ {source_name}: {result}")
                failed_sources.append(source_name)
                collected_data[source_name] = None
            elif result is not None:
                # Validar qualidade dos dados
                validation = self.validate_data_quality(result, source_name)

                if validation['is_valid']:
                    success_sources.append(source_name)
                    collected_data[source_name] = {
                        'data': result,
                        'validation': validation,
                        'data_hash': self.generate_data_hash(result),
                        'collection_timestamp': datetime.now().isoformat()
                    }
                else:
                    self.logger.warning(f"⚠️ {source_name}: Dados com problemas de qualidade")
                    failed_sources.append(source_name)
                    collected_data[source_name] = {
                        'data': result,
                        'validation': validation,
                        'data_hash': self.generate_data_hash(result),
                        'collection_timestamp': datetime.now().isoformat()
                    }
            else:
                self.logger.warning(f"⚠️ {source_name}: Nenhum dado coletado")
                failed_sources.append(source_name)
                collected_data[source_name] = None

        # Consolidar dados
        comprehensive_data = {
            'metadata': {
                'collection_start': collection_start.isoformat(),
                'collection_end': datetime.now().isoformat(),
                'duration_seconds': (datetime.now() - collection_start).total_seconds(),
                'success_sources': success_sources,
                'failed_sources': failed_sources,
                'total_sources_attempted': len(tasks),
                'total_sources_successful': len(success_sources)
            },
            'sources': collected_data,
            'primary_data': collected_data.get('blockchair', {}).get('data', {}) if collected_data.get('blockchair') else {}
        }

        self.logger.info(
            f"✅ Coleta concluída. "
            f"Sucesso: {len(success_sources)}/{len(tasks)} fontes. "
            f"Duração: {comprehensive_data['metadata']['duration_seconds']:.2f}s"
        )

        self.collected_data = comprehensive_data
        return comprehensive_data

    def save_raw_data(self, data: Dict, filename_suffix: str = "") -> str:
        """
        Salva dados brutos para análise posterior
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            suffix = f"_{filename_suffix}" if filename_suffix else ""
            filename = f"data/raw/blockchain/collector_raw_{timestamp}{suffix}.json"

            os.makedirs('data/raw/blockchain', exist_ok=True)

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)

            self.logger.info(f"💾 Dados brutos salvos: {filename}")
            return filename

        except Exception as e:
            self.logger.error(f"❌ Erro ao salvar dados brutos: {e}")
            return ""

    def get_primary_metrics(self) -> Dict:
        """
        Extrai métricas primárias dos dados coletados (para compatibilidade)
        """
        if not self.collected_data or 'primary_data' not in self.collected_data:
            return {}

        primary_data = self.collected_data['primary_data']

        # Mapear campos para formato compatível com o dashboard atual
        metrics = {
            'blocks': primary_data.get('blocks'),
            'transactions': primary_data.get('transactions'),
            'outputs': primary_data.get('outputs'),
            'circulation': primary_data.get('circulation'),
            'blocks_24h': primary_data.get('blocks_24h'),
            'transactions_24h': primary_data.get('transactions_24h'),
            'difficulty': primary_data.get('difficulty'),
            'volume_24h': primary_data.get('volume_24h'),
            'mempool_transactions': primary_data.get('mempool_transactions'),
            'mempool_size': primary_data.get('mempool_size'),
            'mempool_tps': primary_data.get('mempool_tps'),
            'best_block_height': primary_data.get('best_block_height'),
            'best_block_hash': primary_data.get('best_block_hash'),
            'best_block_time': primary_data.get('best_block_time'),
            'blockchain_size': primary_data.get('blockchain_size'),
            'average_transaction_fee_24h': primary_data.get('average_transaction_fee_24h'),
            'median_transaction_fee_24h': primary_data.get('median_transaction_fee_24h'),
            'suggested_transaction_fee_per_byte_sat': primary_data.get('suggested_transaction_fee_per_byte_sat'),
            'nodes': primary_data.get('nodes'),
            'hashrate_24h': primary_data.get('hashrate_24h'),
            'market_price_usd': primary_data.get('market_price_usd'),
            'market_cap_usd': primary_data.get('market_cap_usd'),
            'market_dominance_percentage': primary_data.get('market_dominance_percentage'),
            'next_difficulty_estimate': primary_data.get('next_difficulty_estimate'),
            'hodling_addresses': primary_data.get('hodling_addresses'),
        }

        # Filtrar valores None
        return {k: v for k, v in metrics.items() if v is not None}

# Funções de conveniência para uso rápido
async def collect_blockchain_data() -> Dict:
    """
    Função simplificada para coleta rápida de dados
    """
    async with BlockchainCollector() as collector:
        return await collector.collect_from_all_sources()

async def get_primary_metrics() -> Dict:
    """
    Função simplificada para obter apenas métricas primárias
    """
    async with BlockchainCollector() as collector:
        await collector.collect_from_all_sources()
        return collector.get_primary_metrics()

# Teste do coletor
async def test_collector():
    """Função de teste para verificar o coletor"""
    print("🧪 Testando BlockchainCollector...")

    async with BlockchainCollector() as collector:
        data = await collector.collect_from_all_sources()

        print(f"✅ Coleta concluída:")
        print(f"   - Fontes bem-sucedidas: {data['metadata']['success_sources']}")
        print(f"   - Fontes com falha: {data['metadata']['failed_sources']}")
        print(f"   - Duração: {data['metadata']['duration_seconds']:.2f}s")

        # Salvar dados de teste
        filename = collector.save_raw_data(data, "test")
        print(f"   - Dados salvos: {filename}")

        # Mostrar métricas primárias
        metrics = collector.get_primary_metrics()
        print(f"   - Métricas coletadas: {len(metrics)} campos")

        return data

if __name__ == "__main__":
    # Executar teste se rodado diretamente
    data = asyncio.run(test_collector())
