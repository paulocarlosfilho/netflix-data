FROM python:3.11-slim

WORKDIR /app

# Instala dependências do sistema para o psycopg2
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

# Copia e instala apenas o necessário
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código do dashboard
COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]