from flask import Flask
from app.routes.terceirizados import terceirizados_bp
from app.db import initialize_duckdb
from flasgger import Swagger


def create_app():
    app = Flask(__name__)

    # Inicializa banco ao subir aplicação
    initialize_duckdb()
    Swagger(app)

    app.register_blueprint(terceirizados_bp)

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
