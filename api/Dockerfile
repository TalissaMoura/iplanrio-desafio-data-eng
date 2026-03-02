FROM python:3.12-slim

# Evita arquivos .pyc e força logs imediatos
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Instala dependências do sistema mínimas
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copia apenas arquivos necessários para instalar dependências
COPY pyproject.toml .

# Instala o projeto
RUN pip install --upgrade pip && \
    pip install .

# Copia código da aplicação
COPY app ./app

# (Opcional) copia banco se você for embedar
# COPY data ./data

EXPOSE 8000

CMD ["python", "-m","app.main"]
