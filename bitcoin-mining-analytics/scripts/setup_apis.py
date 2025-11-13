#!/usr/bin/env python3
"""
Script de configuração de APIs para Bitcoin Mining Analytics
"""

import os
import yaml
from getpass import getpass

def setup_apis():
    """Configura as chaves de API interativamente"""
    print("🔑 CONFIGURAÇÃO DE APIs - BITCOIN MINING ANALYTICS")
    print("=" * 50)

    config_path = "config/data_sources.yaml"

    # Carrega configuração existente ou cria nova
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    else:
        config = {
            'api_keys': {},
            'endpoints': {},
            'rate_limits': {},
            'timeouts': {}
        }

    print("\n📝 Configure suas chaves de API (Enter para pular):")

    # Blockchain.com
    blockchain_key = getpass("Blockchain.com API Key: ")
    if blockchain_key:
        config['api_keys']['blockchain_com'] = blockchain_key

    # Glassnode
    glassnode_key = getpass("Glassnode API Key: ")
    if glassnode_key:
        config['api_keys']['glassnode'] = glassnode_key

    # CryptoCompare
    cc_key = getpass("CryptoCompare API Key: ")
    if cc_key:
        config['api_keys']['cryptocompare'] = cc_key

    # Salva configuração
    with open(config_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, indent=2)

    print(f"\n✅ Configuração salva em: {config_path}")
    print("\n🔗 URLs para obter chaves:")
    print("   Blockchain.com: https://www.blockchain.com/api/developers")
    print("   Glassnode: https://glassnode.com/")
    print("   CryptoCompare: https://www.cryptocompare.com/")

    # Testa conexões básicas
    test_basic_apis()

def test_basic_apis():
    """Testa APIs que não precisam de chave"""
    import aiohttp
    import asyncio

    async def test_coingecko():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        price = data.get('bitcoin', {}).get('usd', 0)
                        print(f"✅ CoinGecko: Preço BTC = ${price:,.2f}")
                    else:
                        print("❌ CoinGecko: Falha na conexão")
        except Exception as e:
            print(f"❌ CoinGecko: {e}")

    async def test_blockchair():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://api.blockchair.com/bitcoin/stats"
                ) as response:
                    if response.status == 200:
                        print("✅ Blockchair: Conexão OK")
                    else:
                        print("❌ Blockchair: Falha na conexão")
        except Exception as e:
            print(f"❌ Blockchair: {e}")

    print("\n🧪 Testando conexões com APIs gratuitas...")
    asyncio.run(test_coingecko())
    asyncio.run(test_blockchair())

if __name__ == "__main__":
    setup_apis()
