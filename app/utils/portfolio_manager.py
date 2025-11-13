import json
import os
from datetime import datetime

class WebPortfolioManager:
    def __init__(self, portfolio_file='data/portfolios.json'):
        self.portfolio_file = portfolio_file
        self.portfolios = self._load_portfolios()

    def _load_portfolios(self):
        try:
            if os.path.exists(self.portfolio_file):
                with open(self.portfolio_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                return {"portfolios": {}}
        except:
            return {"portfolios": {}}

    def _save_portfolios(self):
        try:
            with open(self.portfolio_file, 'w', encoding='utf-8') as f:
                json.dump(self.portfolios, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Erro ao salvar portfolios: {e}")

    def create_portfolio(self, name, owner, initial_investment=0):
        portfolio_id = f"{owner}_{name}".lower().replace(" ", "_")

        if portfolio_id in self.portfolios.get("portfolios", {}):
            return False

        new_portfolio = {
            "name": name,
            "owner": owner,
            "initial_investment": initial_investment,
            "allocations": {},
            "created_at": datetime.now().isoformat()
        }

        self.portfolios["portfolios"][portfolio_id] = new_portfolio
        self._save_portfolios()
        return True

    def add_investment(self, portfolio_id, asset, amount, price=None):
        if portfolio_id not in self.portfolios.get("portfolios", {}):
            return False

        portfolio = self.portfolios["portfolios"][portfolio_id]

        if asset in portfolio["allocations"]:
            portfolio["allocations"][asset]["amount"] += amount
        else:
            portfolio["allocations"][asset] = {
                "amount": amount,
                "average_price": price,
                "added_at": datetime.now().isoformat()
            }

        self._save_portfolios()
        return True

    def list_portfolios(self):
        return self.portfolios.get("portfolios", {})

    def calculate_portfolio_value(self, portfolio_data):
        total_value = portfolio_data.get("initial_investment", 0)
        allocations = portfolio_data.get("allocations", {})

        for asset, allocation in allocations.items():
            if isinstance(allocation, dict) and "amount" in allocation:
                total_value += allocation["amount"]

        return total_value
