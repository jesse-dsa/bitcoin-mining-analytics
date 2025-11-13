from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
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
