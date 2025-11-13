#!/usr/bin/env python3
"""
BITCOIN MINING PIPELINE - VERSÃO COM BLOCKCHAIR CORRIGIDO
Debug completo da API Blockchair
"""

import logging
import asyncio
import aiohttp
from datetime import datetime
import json

logging.basicConfig(level=logging.DEBUG, format='%(message)s')
logger = logging.getLogger(__name__)

async def get_real_blockchain_data():
    """Coleta dados REAIS do Blockchair com debug completo"""
    try:
        async with aiohttp.ClientSession() as session:
            url = "https://api.blockchair.com/bitcoin/stats"
            logger.debug(f"🌐 Conectando: {url}")

            async with session.get(url, timeout=15) as response:
                logger.debug(f"📡 Status: {response.status}")

                if response.status == 200:
                    data = await response.json()
                    logger.debug(f"📊 Resposta completa: {json.dumps(data, indent=2)}")

                    stats = data.get('data', {})

                    # 🔍 DEBUG: Mostra TODOS os campos disponíveis
                    logger.debug("🔍 CAMPOS DISPONÍVEIS NO BLOCKCHAIR:")
                    for key, value in stats.items():
                        logger.debug(f"   {key}: {value} (tipo: {type(value)})")

                    # 🎯 EXTRAÇÃO CORRETA - hash rate pode ser string!
                    hash_rate_raw = stats.get('hashrate_24h', '0')
                    logger.debug(f"🔧 Hash Rate Bruto: {hash_rate_raw} (tipo: {type(hash_rate_raw)})")

                    # Converter para número
                    try:
                        if isinstance(hash_rate_raw, str):
                            hash_rate_hs = float(hash_rate_raw)
                        else:
                            hash_rate_hs = float(hash_rate_raw)

                        hash_rate_ph = hash_rate_hs / 1e12  # H/s para PH/s

                        logger.debug(f"🧮 Hash Rate Convertido: {hash_rate_hs:,.0f} H/s → {hash_rate_ph:,.2f} PH/s")

                    except (ValueError, TypeError) as e:
                        logger.error(f"❌ Erro na conversão: {e}")
                        return None

                    return {
                        'hash_rate_ph': hash_rate_ph,
                        'hash_rate_hs': hash_rate_hs,
                        'difficulty': stats.get('difficulty', 0),
                        'blocks': stats.get('blocks', 0),
                        'transactions': stats.get('transactions', 0),
                        'mempool_size': stats.get('mempool_size', 0),
                        'mempool_transactions': stats.get('mempool_transactions', 0),
                        'realtime': True,
                        'source': 'blockchair',
                        'raw_data': stats  # Para debug
                    }
                else:
                    logger.error(f"❌ HTTP {response.status}")
                    return None

    except Exception as e:
        logger.error(f"❌ Erro geral: {e}")
        import traceback
        logger.error(f"🔍 Stack trace: {traceback.format_exc()}")
        return None

async def main():
    print("🔧 TESTE BLOCKCHAIR - DEBUG COMPLETO")
    print("=" * 50)

    data = await get_real_blockchain_data()

    print("\n📊 RESULTADO FINAL:")
    print("=" * 30)

    if data and data['realtime']:
        print(f"✅ Hash Rate: {data['hash_rate_ph']:,.2f} PH/s")
        print(f"✅ Hash Rate (H/s): {data['hash_rate_hs']:,.0f} H/s")
        print(f"✅ Dificuldade: {data['difficulty']:,.0f}")
        print(f"✅ Blocks: {data['blocks']:,}")
        print(f"✅ Transações: {data['transactions']:,}")
        print(f"✅ Mempool: {data['mempool_transactions']:,} tx")
        print(f"✅ Fonte: {data['source']}")

        # 🎯 COMPARAÇÃO COM FALLBACK
        print(f"\n🔍 COMPARAÇÃO:")
        print(f"   Fallback: 450,000 PH/s")
        print(f"   Real:     {data['hash_rate_ph']:,.0f} PH/s")
        print(f"   Diferença: {((data['hash_rate_ph'] / 450000) - 1) * 100:+.1f}%")

    else:
        print("❌ Não conseguiu dados reais do Blockchair")
        print("💡 Usando dados de fallback no pipeline")

if __name__ == "__main__":
    asyncio.run(main())
