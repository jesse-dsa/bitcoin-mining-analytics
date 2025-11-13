# scripts/portfolio_manager.py
import json
import os
from datetime import datetime
from typing import Dict, List

class PortfolioManager:
    """Gerenciador dinâmico de portfolios de criptomoedas"""

    def __init__(self, portfolio_file: str = "data/portfolios.json"):
        self.portfolio_file = portfolio_file
        self.portfolios = self._load_portfolios()

    def _load_portfolios(self) -> Dict:
        """Carrega portfolios do arquivo JSON"""
        try:
            if os.path.exists(self.portfolio_file):
                with open(self.portfolio_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                # Criar estrutura inicial
                os.makedirs(os.path.dirname(self.portfolio_file), exist_ok=True)
                initial_data = {
                    "portfolios": {},
                    "last_updated": datetime.now().isoformat()
                }
                self._save_portfolios(initial_data)
                return initial_data
        except Exception as e:
            print(f"❌ Erro ao carregar portfolios: {e}")
            return {"portfolios": {}}

    def _save_portfolios(self, data: Dict):
        """Salva portfolios no arquivo JSON"""
        try:
            with open(self.portfolio_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"❌ Erro ao salvar portfolios: {e}")

    def create_portfolio(self, name: str, owner: str, initial_investment: float = 0):
        """Cria um novo portfolio"""
        portfolio_id = f"{owner}_{name}".lower().replace(" ", "_")

        if portfolio_id in self.portfolios.get("portfolios", {}):
            print(f"⚠️  Portfolio '{name}' já existe para {owner}")
            return False

        new_portfolio = {
            "name": name,
            "owner": owner,
            "initial_investment": initial_investment,
            "allocations": {},
            "created_at": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat()
        }

        self.portfolios["portfolios"][portfolio_id] = new_portfolio
        self.portfolios["last_updated"] = datetime.now().isoformat()
        self._save_portfolios(self.portfolios)

        print(f"✅ Portfolio '{name}' criado para {owner}")
        return True

    def add_investment(self, portfolio_id: str, asset: str, amount: float, price: float = None):
        """Adiciona um investimento a um portfolio"""
        if portfolio_id not in self.portfolios.get("portfolios", {}):
            print(f"❌ Portfolio '{portfolio_id}' não encontrado")
            return False

        portfolio = self.portfolios["portfolios"][portfolio_id]

        if asset in portfolio["allocations"]:
            # Atualizar investimento existente
            portfolio["allocations"][asset]["amount"] += amount
            if price:
                portfolio["allocations"][asset]["average_price"] = (
                    (portfolio["allocations"][asset]["average_price"] *
                     portfolio["allocations"][asset]["amount"] + price * amount) /
                    (portfolio["allocations"][asset]["amount"] + amount)
                )
        else:
            # Novo investimento
            portfolio["allocations"][asset] = {
                "amount": amount,
                "average_price": price if price else 0,
                "added_at": datetime.now().isoformat()
            }

        portfolio["last_updated"] = datetime.now().isoformat()
        self.portfolios["last_updated"] = datetime.now().isoformat()
        self._save_portfolios(self.portfolios)

        print(f"✅ ${amount:,.2f} em {asset} adicionado ao portfolio {portfolio_id}")
        return True

    def get_portfolio(self, portfolio_id: str):
        """Obtém um portfolio específico"""
        return self.portfolios.get("portfolios", {}).get(portfolio_id)

    def list_portfolios(self):
        """Lista todos os portfolios"""
        return self.portfolios.get("portfolios", {})

    def delete_portfolio(self, portfolio_id: str):
        """Remove um portfolio"""
        if portfolio_id in self.portfolios.get("portfolios", {}):
            del self.portfolios["portfolios"][portfolio_id]
            self.portfolios["last_updated"] = datetime.now().isoformat()
            self._save_portfolios(self.portfolios)
            print(f"✅ Portfolio '{portfolio_id}' removido")
            return True
        else:
            print(f"❌ Portfolio '{portfolio_id}' não encontrado")
            return False

# Exemplo de uso rápido
def setup_sample_portfolios():
    """Configura alguns portfolios de exemplo"""
    manager = PortfolioManager()

    # Seu portfolio
    manager.create_portfolio("Principal", "Você", 1000)
    manager.add_investment("você_principal", "BTC", 500, 65000)
    manager.add_investment("você_principal", "ETH", 250, 3250)
    manager.add_investment("você_principal", "LINK", 250, 19.5)

    # Portfolio do seu amigo Carlos
    manager.create_portfolio("Trading", "Carlos", 1000)
    manager.add_investment("carlos_trading", "BTC", 500, 65000)
    manager.add_investment("carlos_trading", "ETH", 250, 3250)
    manager.add_investment("carlos_trading", "LINK", 250, 19.5)

    print("✅ Portfolios de exemplo criados!")

if __name__ == "__main__":
    setup_sample_portfolios()
