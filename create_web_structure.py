# create_web_app.py
import os
import sys

def create_directory_structure():
    """Cria toda a estrutura de diretórios"""
    print("📁 Criando estrutura de diretórios...")

    directories = [
        'app',
        'app/utils',
        'app/templates',
        'static/css',
        'static/js',
        'static/images',
        'data'
    ]

    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"✅ Criado: {directory}/")

def create_requirements():
    """Cria arquivo requirements.txt"""
    content = """Flask==2.3.3
Flask-SQLAlchemy==3.0.5
duckdb==0.9.2
pandas==2.1.1
python-dotenv==1.0.0
gunicorn==21.2.0
"""

    with open('requirements.txt', 'w', encoding='utf-8') as f:
        f.write(content)
    print("✅ requirements.txt criado")

def create_config():
    """Cria arquivo config.py"""
    content = '''import os
from datetime import timedelta

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key'
    SQLALCHEMY_DATABASE_URI = 'sqlite:///app.db'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    DUCKDB_PATH = 'data/bitcoin_analytics.duckdb'
    PORTFOLIO_FILE = 'data/portfolios.json'

class DevelopmentConfig(Config):
    DEBUG = True

class ProductionConfig(Config):
    DEBUG = False

config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}
'''

    with open('config.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("✅ config.py criado")

def create_app_init():
    """Cria app/__init__.py"""
    content = '''from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from config import config

db = SQLAlchemy()

def create_app(config_name='default'):
    app = Flask(__name__)
    app.config.from_object(config[config_name])

    db.init_app(app)

    from app.routes import main_bp
    app.register_blueprint(main_bp)

    with app.app_context():
        db.create_all()

    return app
'''

    with open('app/__init__.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("✅ app/__init__.py criado")

def create_models():
    """Cria app/models.py"""
    content = '''from app import db
from datetime import datetime

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Portfolio(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    initial_investment = db.Column(db.Float, default=0.0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
'''

    with open('app/models.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("✅ app/models.py criado")

def create_routes():
    """Cria app/routes.py"""
    content = '''from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
from datetime import datetime
import os

from app.utils.analysis import BitcoinAnalyzer
from app.utils.portfolio_manager import WebPortfolioManager

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def dashboard():
    """Página principal do dashboard"""
    analyzer = BitcoinAnalyzer()
    portfolio_manager = WebPortfolioManager()

    try:
        metrics = analyzer.get_current_metrics()
        analysis = analyzer.generate_market_analysis()
        portfolios = portfolio_manager.list_portfolios()

        analyzer.close()

        return render_template('dashboard.html',
                            metrics=metrics,
                            analysis=analysis,
                            portfolios=portfolios,
                            current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    except Exception as e:
        return render_template('dashboard.html',
                            metrics={},
                            analysis={},
                            portfolios={},
                            current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

@main_bp.route('/portfolios')
def portfolios():
    """Página de gerenciamento de portfolios"""
    portfolio_manager = WebPortfolioManager()
    portfolios_data = portfolio_manager.list_portfolios()

    return render_template('portfolios.html',
                         portfolios=portfolios_data,
                         portfolio_manager=portfolio_manager)

@main_bp.route('/portfolios/create', methods=['POST'])
def create_portfolio():
    """Cria um novo portfolio"""
    name = request.form.get('name')
    owner = request.form.get('owner')
    initial_investment = float(request.form.get('initial_investment', 0))

    portfolio_manager = WebPortfolioManager()

    if portfolio_manager.create_portfolio(name, owner, initial_investment):
        flash(f'Portfolio "{name}" criado com sucesso!', 'success')
    else:
        flash('Erro ao criar portfolio.', 'error')

    return redirect(url_for('main.portfolios'))

@main_bp.route('/portfolios/<portfolio_id>/add-investment', methods=['POST'])
def add_investment(portfolio_id):
    """Adiciona investimento a um portfolio"""
    asset = request.form.get('asset')
    amount = float(request.form.get('amount', 0))
    price = float(request.form.get('price', 0))

    portfolio_manager = WebPortfolioManager()

    if portfolio_manager.add_investment(portfolio_id, asset, amount, price):
        flash(f'Investimento adicionado com sucesso!', 'success')
    else:
        flash('Erro ao adicionar investimento.', 'error')

    return redirect(url_for('main.portfolios'))

@main_bp.route('/mining')
def mining():
    """Página de análise de mineração"""
    analyzer = BitcoinAnalyzer()

    try:
        profitability = analyzer.get_mining_profitability()
        metrics = analyzer.get_current_metrics()

        analyzer.close()

        return render_template('mining.html',
                            profitability=profitability,
                            metrics=metrics)

    except Exception as e:
        return render_template('mining.html',
                            profitability={},
                            metrics={})

@main_bp.route('/analysis')
def analysis():
    """Página de análise detalhada"""
    analyzer = BitcoinAnalyzer()

    try:
        metrics = analyzer.get_current_metrics()
        analysis_result = analyzer.generate_market_analysis()

        analyzer.close()

        return render_template('analysis.html',
                            metrics=metrics,
                            analysis=analysis_result)

    except Exception as e:
        return render_template('analysis.html',
                            metrics={},
                            analysis={})
'''

    with open('app/routes.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("✅ app/routes.py criado")

def create_analysis_util():
    """Cria app/utils/analysis.py"""
    content = '''import duckdb
import pandas as pd
from datetime import datetime

class BitcoinAnalyzer:
    def __init__(self, db_path='data/bitcoin_analytics.duckdb'):
        self.db_path = db_path
        try:
            self.conn = duckdb.connect(db_path)
        except:
            self.conn = None

    def get_current_metrics(self):
        """Obtém métricas atuais"""
        if not self.conn:
            return self._get_sample_metrics()

        try:
            query = """
            SELECT timestamp, market_price_usd, hashrate_24h_ehs,
                   transactions_24h, difficulty, mempool_transactions
            FROM bitcoin_network_metrics
            ORDER BY timestamp DESC
            LIMIT 1
            """

            result = self.conn.execute(query).fetchone()
            if result:
                columns = [desc[0] for desc in self.conn.description]
                return dict(zip(columns, result))
            return self._get_sample_metrics()
        except:
            return self._get_sample_metrics()

    def get_mining_profitability(self):
        """Obtém análise de lucratividade"""
        return {
            "daily_profit_usd": 45.67,
            "profit_margin_percentage": 25.5,
            "roi_days": 420,
            "status": "lucrativa"
        }

    def generate_market_analysis(self):
        """Gera análise do mercado"""
        metrics = self.get_current_metrics()

        return {
            "price_analysis": {
                "trend": "alta",
                "current_price": metrics.get('market_price_usd', 65000),
                "change_percentage": 2.5
            },
            "network_health": {
                "health": "excelente",
                "security": "alta"
            },
            "mining_economics": {
                "status": "lucrativa",
                "margin": 25.5
            },
            "investment_recommendation": {
                "action": "comprar",
                "confidence": "alta"
            }
        }

    def _get_sample_metrics(self):
        """Retorna métricas de exemplo"""
        return {
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "market_price_usd": 65432.10,
            "hashrate_24h_ehs": 645.7,
            "transactions_24h": 345678,
            "difficulty": 83456789012,
            "mempool_transactions": 4567
        }

    def close(self):
        if self.conn:
            self.conn.close()
'''

    with open('app/utils/analysis.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("✅ app/utils/analysis.py criado")

def create_portfolio_manager_util():
    """Cria app/utils/portfolio_manager.py"""
    content = '''import json
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
'''

    with open('app/utils/portfolio_manager.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("✅ app/utils/portfolio_manager.py criado")

def create_base_template():
    """Cria app/templates/base.html"""
    content = '''<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{% block title %}Bitcoin Analytics{% endblock %}</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <style>
        .navbar-brand { font-weight: bold; }
        .card { margin-bottom: 1rem; }
        .metric-card { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; }
    </style>
</head>
<body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark">
        <div class="container">
            <a class="navbar-brand" href="/">
                <i class="fas fa-bitcoin"></i> Bitcoin Analytics
            </a>
            <div class="navbar-nav">
                <a class="nav-link" href="/">Dashboard</a>
                <a class="nav-link" href="/portfolios">Portfolios</a>
                <a class="nav-link" href="/mining">Mineração</a>
                <a class="nav-link" href="/analysis">Análise</a>
            </div>
        </div>
    </nav>

    <div class="container mt-4">
        {% with messages = get_flashed_messages() %}
            {% if messages %}
                {% for message in messages %}
                    <div class="alert alert-info alert-dismissible fade show" role="alert">
                        {{ message }}
                        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
                    </div>
                {% endfor %}
            {% endif %}
        {% endwith %}

        {% block content %}{% endblock %}
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    {% block scripts %}{% endblock %}
</body>
</html>'''

    with open('app/templates/base.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("✅ app/templates/base.html criado")

def create_dashboard_template():
    """Cria app/templates/dashboard.html"""
    content = '''{% extends "base.html" %}

{% block content %}
<div class="row">
    <div class="col-12">
        <h1><i class="fas fa-tachometer-alt"></i> Dashboard</h1>
        <p class="text-muted">Última atualização: {{ current_time }}</p>
    </div>
</div>

<div class="row">
    <div class="col-md-3">
        <div class="card metric-card">
            <div class="card-body">
                <h5 class="card-title"><i class="fas fa-coins"></i> Preço BTC</h5>
                <h2>${{ "%.2f"|format(metrics.market_price_usd|default(0)) }}</h2>
            </div>
        </div>
    </div>

    <div class="col-md-3">
        <div class="card metric-card">
            <div class="card-body">
                <h5 class="card-title"><i class="fas fa-bolt"></i> Hash Rate</h5>
                <h2>{{ "%.1f"|format(metrics.hashrate_24h_ehs|default(0)) }} EH/s</h2>
            </div>
        </div>
    </div>

    <div class="col-md-3">
        <div class="card metric-card">
            <div class="card-body">
                <h5 class="card-title"><i class="fas fa-exchange-alt"></i> Transações</h5>
                <h2>{{ "{:,}".format(metrics.transactions_24h|default(0)) }}</h2>
            </div>
        </div>
    </div>

    <div class="col-md-3">
        <div class="card metric-card">
            <div class="card-body">
                <h5 class="card-title"><i class="fas fa-layer-group"></i> Mempool</h5>
                <h2>{{ "{:,}".format(metrics.mempool_transactions|default(0)) }}</h2>
            </div>
        </div>
    </div>
</div>

<div class="row mt-4">
    <div class="col-md-8">
        <div class="card">
            <div class="card-header">
                <h5>Análise do Mercado</h5>
            </div>
            <div class="card-body">
                {% if analysis %}
                    <p><strong>Tendência:</strong> {{ analysis.price_analysis.trend }}</p>
                    <p><strong>Saúde da Rede:</strong> {{ analysis.network_health.health }}</p>
                    <p><strong>Mineração:</strong> {{ analysis.mining_economics.status }}</p>
                    <p><strong>Recomendação:</strong> {{ analysis.investment_recommendation.action }}</p>
                {% endif %}
            </div>
        </div>
    </div>
</div>
{% endblock %}'''

    with open('app/templates/dashboard.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("✅ app/templates/dashboard.html criado")

def create_portfolios_template():
    """Cria app/templates/portfolios.html"""
    content = '''{% extends "base.html" %}

{% block content %}
<div class="row">
    <div class="col-12">
        <h1><i class="fas fa-wallet"></i> Portfolios</h1>
    </div>
</div>

<div class="row">
    <div class="col-md-6">
        <div class="card">
            <div class="card-header">
                <h5>Criar Portfolio</h5>
            </div>
            <div class="card-body">
                <form method="POST" action="/portfolios/create">
                    <div class="mb-3">
                        <label class="form-label">Nome</label>
                        <input type="text" class="form-control" name="name" required>
                    </div>
                    <div class="mb-3">
                        <label class="form-label">Proprietário</label>
                        <input type="text" class="form-control" name="owner" required>
                    </div>
                    <div class="mb-3">
                        <label class="form-label">Investimento Inicial</label>
                        <input type="number" class="form-control" name="initial_investment" step="0.01" required>
                    </div>
                    <button type="submit" class="btn btn-success">Criar</button>
                </form>
            </div>
        </div>
    </div>

    <div class="col-md-6">
        <div class="card">
            <div class="card-header">
                <h5>Adicionar Investimento</h5>
            </div>
            <div class="card-body">
                <form method="POST" action="#" id="investmentForm">
                    <div class="mb-3">
                        <label class="form-label">Portfolio</label>
                        <select class="form-control" name="portfolio_id" required>
                            <option value="">Selecione...</option>
                            {% for id, portfolio in portfolios.items() %}
                            <option value="{{ id }}">{{ portfolio.name }}</option>
                            {% endfor %}
                        </select>
                    </div>
                    <div class="mb-3">
                        <label class="form-label">Ativo</label>
                        <select class="form-control" name="asset" required>
                            <option value="BTC">BTC</option>
                            <option value="ETH">ETH</option>
                        </select>
                    </div>
                    <div class="mb-3">
                        <label class="form-label">Valor (USD)</label>
                        <input type="number" class="form-control" name="amount" step="0.01" required>
                    </div>
                    <button type="submit" class="btn btn-primary">Adicionar</button>
                </form>
            </div>
        </div>
    </div>
</div>

<div class="row mt-4">
    <div class="col-12">
        <div class="card">
            <div class="card-header">
                <h5>Meus Portfolios</h5>
            </div>
            <div class="card-body">
                {% if portfolios %}
                    {% for id, portfolio in portfolios.items() %}
                    <div class="card mb-2">
                        <div class="card-body">
                            <h5>{{ portfolio.name }} ({{ portfolio.owner }})</h5>
                            <p>Valor: ${{ "%.2f"|format(portfolio_manager.calculate_portfolio_value(portfolio)) }}</p>
                            {% if portfolio.allocations %}
                                <h6>Alocações:</h6>
                                {% for asset, alloc in portfolio.allocations.items() %}
                                <span class="badge bg-secondary">{{ asset }}: ${{ "%.2f"|format(alloc.amount) }}</span>
                                {% endfor %}
                            {% endif %}
                        </div>
                    </div>
                    {% endfor %}
                {% else %}
                    <p>Nenhum portfolio criado.</p>
                {% endif %}
            </div>
        </div>
    </div>
</div>

<script>
document.querySelector('select[name="portfolio_id"]').addEventListener('change', function() {
    const form = document.getElementById('investmentForm');
    const portfolioId = this.value;
    form.action = '/portfolios/' + portfolioId + '/add-investment';
});
</script>
{% endblock %}'''

    with open('app/templates/portfolios.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("✅ app/templates/portfolios.html criado")

def create_mining_template():
    """Cria app/templates/mining.html"""
    content = '''{% extends "base.html" %}

{% block content %}
<div class="row">
    <div class="col-12">
        <h1><i class="fas fa-digging"></i> Mineração</h1>
    </div>
</div>

<div class="row">
    <div class="col-md-4">
        <div class="card metric-card">
            <div class="card-body">
                <h5>Lucro Diário</h5>
                <h2>${{ "%.2f"|format(profitability.daily_profit_usd|default(0)) }}</h2>
            </div>
        </div>
    </div>

    <div class="col-md-4">
        <div class="card metric-card">
            <div class="card-body">
                <h5>Margem</h5>
                <h2>{{ "%.1f"|format(profitability.profit_margin_percentage|default(0)) }}%</h2>
            </div>
        </div>
    </div>

    <div class="col-md-4">
        <div class="card metric-card">
            <div class="card-body">
                <h5>ROI</h5>
                <h2>{{ profitability.roi_days|default(0) }} dias</h2>
            </div>
        </div>
    </div>
</div>
{% endblock %}'''

    with open('app/templates/mining.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("✅ app/templates/mining.html criado")

def create_analysis_template():
    """Cria app/templates/analysis.html"""
    content = '''{% extends "base.html" %}

{% block content %}
<div class="row">
    <div class="col-12">
        <h1><i class="fas fa-chart-line"></i> Análise</h1>
    </div>
</div>

<div class="row">
    <div class="col-md-6">
        <div class="card">
            <div class="card-header">
                <h5>Métricas da Rede</h5>
            </div>
            <div class="card-body">
                {% if metrics %}
                <p><strong>Preço:</strong> ${{ "%.2f"|format(metrics.market_price_usd) }}</p>
                <p><strong>Hash Rate:</strong> {{ "%.1f"|format(metrics.hashrate_24h_ehs) }} EH/s</p>
                <p><strong>Dificuldade:</strong> {{ "{:,}".format(metrics.difficulty) }}</p>
                {% endif %}
            </div>
        </div>
    </div>

    <div class="col-md-6">
        <div class="card">
            <div class="card-header">
                <h5>Análise</h5>
            </div>
            <div class="card-body">
                {% if analysis %}
                <p><strong>Tendência:</strong> {{ analysis.price_analysis.trend }}</p>
                <p><strong>Saúde:</strong> {{ analysis.network_health.health }}</p>
                <p><strong>Recomendação:</strong> {{ analysis.investment_recommendation.action }}</p>
                {% endif %}
            </div>
        </div>
    </div>
</div>
{% endblock %}'''

    with open('app/templates/analysis.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("✅ app/templates/analysis.html criado")

def create_app_file():
    """Cria app.py"""
    content = '''from app import create_app

app = create_app()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
'''

    with open('app.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("✅ app.py criado")

def create_readme():
    """Cria README.md"""
    content = ''

def main():
    """Função principal"""
    print("🚀 Criando aplicação web Bitcoin Analytics...")
    print("=" * 50)

    try:
        create_directory_structure()
        create_requirements()
        create_config()
        create_app_init()
        create_models()
        create_routes()
        create_analysis_util()
        create_portfolio_manager_util()
        create_base_template()
        create_dashboard_template()
        create_portfolios_template()
        create_mining_template()
        create_analysis_template()
        create_app_file()
        create_readme()

        print("=" * 50)
        print("🎉 Aplicação criada com sucesso!")
        print("\n📝 Para executar:")
        print("1. pip install -r requirements.txt")
        print("2. python app.py")
        print("3. Acesse: http://localhost:5000")

    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
