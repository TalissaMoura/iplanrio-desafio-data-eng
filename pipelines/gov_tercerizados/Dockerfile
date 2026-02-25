FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Instala uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

# Copia apenas dependências primeiro (cache inteligente)
COPY pyproject.toml .
COPY uv.lock .

RUN uv sync --no-dev

# Copia o restante do projeto
COPY . .

WORKDIR /app/pipelines/gov_tercerizados

CMD ["uv", "run", "prefect", "worker", "start", "--pool", "gov-tercerizados-pool"]
