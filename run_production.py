# run.py (na pasta S:\Cripto)
from app import create_app
from waitress import serve

app = create_app()

if __name__ == '__main__':
    print("🚀 Servidor Waitress iniciado!")
    print("📊 Acesse: http://localhost:5000")
    serve(app, host='0.0.0.0', port=5000)
