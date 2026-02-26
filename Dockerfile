FROM python:3.11-slim

# Definindo o diretório de trabalho na raiz do container
WORKDIR /app

# Instalação de dependências de sistema
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Instala o uv para gerenciamento rápido de pacotes
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 1. Copia primeiro o arquivo de dependências da raiz
COPY pyproject.toml ./

# Instala as dependências (Python + dbt + Prefect)
RUN uv pip install --system --no-cache .

# 2. Copia TODO o projeto para o container

COPY . .

# 3. instala o dbt-deps
RUN dbt deps --project-dir dw --profiles-dir dw/.dbt

# Expõe a variável de ambiente para que o Python encontre seus módulos
ENV PYTHONPATH="/app"


