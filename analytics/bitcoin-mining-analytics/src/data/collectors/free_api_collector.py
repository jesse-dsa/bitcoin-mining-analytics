#!/usr/bin/env python3
"""
COLETOR DE APIs GRATUITAS - CoinGecko + Blockchair
Funciona SEM chaves API!
"""

import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any
import json

logger = logging.getLogger(__name__)

class FreeAPICollector:
    """Coleta dados de APIs gratuitas que não precisam de chaves"""

    def __init__(self, request_timeout: int = 30):
        self.base_urls = {
            'coingecko': 'https://api.coingecko.com/api/v3',
            'blockchair': 'https://api.blockchair.com/bitcoin',
            'mempool_space': 'https://mempool.space/api'
        }
        self.request_timeout = request_timeout

    async def collect_all_free_data(self) -> Dict[str, Any]:
        """Coleta todos os dados das APIs gratuitas"""
        logger.info("🆓 Coletando dados de APIs gratuitas...")

        try:
            # Coleta paralela de todas as fontes gratuitas
            tasks = [
                self.get_bitcoin_price(),
                self.get_blockchain_stats(),
                self.get_network_difficulty(),
                self.get_mempool_data(),
                self.get_mining_pool_distribution()
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Combina resultados
            combined_data = {
                'price_data': results[0],
                'blockchain_stats': results[1],
                'network_difficulty': results[2],
                'mempool_data': results[3],
                'mining_pools': results[4],
                'collection_timestamp': datetime.now(),
                'data_sources': ['coingecko', 'blockchair', 'mempool_space']
            }

            logger.info("✅ Dados de APIs gratuitas coletados com sucesso!")
            return combined_data

        except Exception as e:
            logger.error(f"❌ Erro na coleta de APIs gratuitas: {e}")
            return self.get_fallback_data()

    async def get_bitcoin_price(self) -> Dict[str, Any]:
        """Obtém preço do Bitcoin via CoinGecko (FREE)"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.base_urls['coingecko']}/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_change=true",
                    timeout=self.request_timeout
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        btc_data = data.get('bitcoin', {})

                        return {
                            'timestamp': datetime.now(),
                            'price_usd': btc_data.get('usd', 0),
                            'price_24h_change': btc_data.get('usd_24h_change', 0),
                            'source': 'coingecko',
                            'realtime': True
                        }
            return self.get_fallback_price()

        except Exception as e:
            logger.error(f"Erro ao obter preço Bitcoin: {e}")
            return self.get_fallback_price()

    async def get_blockchain_stats(self) -> Dict[str, Any]:
        """Obtém estatísticas da blockchain via Blockchair (FREE)"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.base_urls['blockchair']}/stats",
                    timeout=self.request_timeout
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        stats = data.get('data', {})

                        return {
                            'timestamp': datetime.now(),
                            'network_hash_rate': stats.get('hashrate_24h', 0),
                            'total_blocks': stats.get('blocks', 0),
                            'total_transactions': stats.get('transactions', 0),
                            'mempool_size': stats.get('mempool_size', 0),
                            'mempool_transactions': stats.get('mempool_transactions', 0),
                            'source': 'blockchair',
                            'realtime': True
                        }
            return self.get_fallback_stats()

        except Exception as e:
            logger.error(f"Erro ao obter stats blockchain: {e}")
            return self.get_fallback_stats()

    async def get_network_difficulty(self) -> Dict[str, Any]:
        """Obtém dificuldade da rede via Blockchair (FREE)"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.base_urls['blockchair']}/stats",
                    timeout=self.request_timeout
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        stats = data.get('data', {})

                        return {
                            'timestamp': datetime.now(),
                            'current_difficulty': stats.get('difficulty', 0),
                            'next_difficulty_estimate': stats.get('difficulty', 0) * 1.02,  # Estimativa
                            'adjustment_blocks_remaining': 345,  # Estimativa
                            'source': 'blockchair',
                            'realtime': True
                        }
            return self.get_fallback_difficulty()

        except Exception as e:
            logger.error(f"Erro ao obter dificuldade: {e}")
            return self.get_fallback_difficulty()

    async def get_mempool_data(self) -> Dict[str, Any]:
        """Obtém dados da mempool via Mempool.space (FREE)"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.base_urls['mempool_space']}/mempool",
                    timeout=self.request_timeout
                ) as response:
                    if response.status == 200:
                        data = await response.json()

                        return {
                            'timestamp': datetime.now(),
                            'mempool_size_bytes': data.get('size', 0),
                            'mempool_transactions': data.get('count', 0),
                            'mempool_usage_percentage': min(data.get('usage', 0) / 300_000_000 * 100, 100),
                            'source': 'mempool_space',
                            'realtime': True
                        }
            return self.get_fallback_mempool()

        except Exception as e:
            logger.error(f"Erro ao obter dados da mempool: {e}")
            return self.get_fallback_mempool()

    async def get_mining_pool_distribution(self) -> Dict[str, Any]:
        """Obtém distribuição de mining pools (dados simulados baseados em tendências)"""
        try:
            # Para pools, usamos dados simulados realistas
            # Em produção, isso viria de APIs específicas
            return {
                'timestamp': datetime.now(),
                'pools': [
                    {'name': 'Foundry USA', 'hash_rate_ph': 158400, 'share_percentage': 35.2},
                    {'name': 'AntPool', 'hash_rate_ph': 129150, 'share_percentage': 28.7},
                    {'name': 'F2Pool', 'hash_rate_ph': 55350, 'share_percentage': 12.3},
                    {'name': 'Binance Pool', 'hash_rate_ph': 40050, 'share_percentage': 8.9},
                    {'name': 'ViaBTC', 'hash_rate_ph': 29250, 'share_percentage': 6.5},
                    {'name': 'Other', 'hash_rate_ph': 37800, 'share_percentage': 8.4}
                ],
                'total_network_hashrate_ph': 450000,
                'source': 'estimated',
                'realtime': False
            }

        except Exception as e:
            logger.error(f"Erro ao obter dados de pools: {e}")
            return self.get_fallback_pools()

    # Métodos de fallback
    def get_fallback_data(self) -> Dict[str, Any]:
        return {
            'price_data': self.get_fallback_price(),
            'blockchain_stats': self.get_fallback_stats(),
            'network_difficulty': self.get_fallback_difficulty(),
            'mempool_data': self.get_fallback_mempool(),
            'mining_pools': self.get_fallback_pools(),
            'fallback_used': True
        }

    def get_fallback_price(self) -> Dict[str, Any]:
        return {
            'timestamp': datetime.now(),
            'price_usd': 102966.0,  # Preço que o CoinGecko retornou!
            'price_24h_change': 2.1,
            'source': 'fallback',
            'realtime': False
        }

    def get_fallback_stats(self) -> Dict[str, Any]:
        return {
            'timestamp': datetime.now(),
            'network_hash_rate': 450000,
            'total_blocks': 820000,
            'total_transactions': 850000000,
            'source': 'fallback',
            'realtime': False
        }

    def get_fallback_difficulty(self) -> Dict[str, Any]:
        return {
            'timestamp': datetime.now(),
            'current_difficulty': 80000000000,
            'source': 'fallback',
            'realtime': False
        }

    def get_fallback_mempool(self) -> Dict[str, Any]:
        return {
            'timestamp': datetime.now(),
            'mempool_size_bytes': 85000000,
            'mempool_transactions': 12500,
            'source': 'fallback',
            'realtime': False
        }

    def get_fallback_pools(self) -> Dict[str, Any]:
        return {
            'timestamp': datetime.now(),
            'pools': [{'name': 'Fallback Pool', 'hash_rate_ph': 450000, 'share_percentage': 100}],
            'total_network_hashrate_ph': 450000,
            'source': 'fallback',
            'realtime': False
        }

# Função de utilidade
async def collect_free_bitcoin_data() -> Dict[str, Any]:
    """Função conveniente para coleta de dados gratuitos"""
    collector = FreeAPICollector()
    return await collector.collect_all_free_data()

if __name__ == "__main__":
    """Teste do coletor gratuito"""
    import asyncio

    async def test_free_collector():
        print("🧪 Testando Coletor de APIs Gratuitas...")

        collector = FreeAPICollector()
        data = await collector.collect_all_free_data()

        print(f"✅ Dados coletados: {len(data)} datasets")
        print(f"💰 Preço BTC: ${data['price_data'].get('price_usd', 0):,.2f}")
        print(f"⛏️  Hash Rate: {data['blockchain_stats'].get('network_hash_rate', 0):,} PH/s")
        print(f"📊 Fontes: {data.get('data_sources', [])}")
        print(f"🕒 Tempo real: {data['price_data'].get('realtime', False)}")

    asyncio.run(test_free_collector())
