FROM python:3.11-slim

ENV POETRY_HOME=/opt/poetry \
    POETRY_VIRTUALENVS_CREATE=false \
    PIP_NO_CACHE_DIR=on \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends curl build-essential && \
    curl -sSL https://install.python-poetry.org | python3 - && \
    ln -s /opt/poetry/bin/poetry /usr/local/bin/poetry && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Impede o Poetry de criar um .venv dentro do container (usa o Python do sistema)
RUN poetry config virtualenvs.create false

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-interaction --no-ansi --only main

COPY . .

CMD ["python", "src/scripts/main.py", "EtapaGold", "--local-scheduler"]
