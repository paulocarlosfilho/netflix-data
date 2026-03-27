# Pipeline de Dados Netflix com Orquestração

Este projeto demonstra a construção de um pipeline de dados de ponta a ponta, utilizando o dataset público (Netflix marketing) como fonte. O pipeline processa dados através de camadas (Bronze, Silver, Gold), valida qualidade com Great Expectations e disponibiliza a camada final tanto em Parquet quanto em PostgreSQL.

## Arquitetura do Projeto

O pipeline segue a arquitetura Medalhão:

-   **Camada Bronze (Bruta):** Download de planilhas do repositório público, enriquecimento com UTM e persistência em Parquet.
-   **Camada Silver (Tratada):** Consolidação, codificação de variáveis e salvamento em Parquet; validado com **Great Expectations**.
-   **Camada Gold (Enriquecida):** Aplicação de um modelo simples de ML (RandomForest) e carga do resultado no **PostgreSQL**.

### Stack Tecnológica

-   **Luigi:** orquestração de tarefas com dependências explícitas; execução local simples e rastreabilidade por etapas.
-   **Pandas:** transformação e limpeza de dados em memória nas camadas Bronze e Silver.
-   **Parquet + PyArrow:** formato colunar eficiente com compressão e tipagem, acelerando leitura/escrita e reduzindo custo de I/O.
-   **Great Expectations:** validação da camada Silver com suíte `silver_netflix_suite` e geração de Data Docs.
-   **scikit-learn:** `RandomForestRegressor` para gerar a coluna `previsao_amount` como baseline de ML.
-   **SQLAlchemy + psycopg2:** persistência da camada Gold no PostgreSQL via `to_sql`, parametrizada por variáveis de ambiente.
-   **python-dotenv:** carregamento de configurações sensíveis a partir de `.env` (ex.: `DATABASE_URL`).
-   **Poetry:** gerenciamento de dependências e execução reprodutível.
-   **Devbox:** shell determinístico para facilitar onboarding e isolamento de ferramentas.
-   **Docker Compose (opcional):** provisionamento rápido do PostgreSQL (e pgAdmin) para desenvolvimento local.

### Por que essas escolhas?

-   **Produtividade local:** Luigi + Pandas + Parquet oferecem ciclo curto de feedback e código direto.
-   **Qualidade objetiva:** Great Expectations formaliza regras e documenta resultados automaticamente.
-   **Entrega consumível:** SQLAlchemy conecta a Gold ao Postgres, pronto para BI e integrações.
-   **Reprodutibilidade:** Devbox + Poetry minimizam divergências entre ambientes; Makefile padroniza passos.

## Como Executar o Projeto

Este projeto foi desenhado para ser 100% reprodutível. Siga os passos abaixo:

### Pré-requisitos

-   [Devbox](https://www.jetpack.io/devbox/docs/installing-devbox/)
-   [Docker](https://docs.docker.com/get-docker/) (opcional, para PostgreSQL)

### 1. Setup Inicial

Primeiro, clone o repositório. Em seguida, execute o script `start` para gerar a estrutura de arquivos de suporte (Devbox, Great Expectations).

```bash
./start
```

### 2. Configurar Banco (opcional)

Crie um arquivo `.env` com a URL do Postgres (se quiser salvar a Gold no banco). Caso não defina, a Gold será salva apenas em Parquet.

```bash
DATABASE_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/netflixdb
# Opcional:
# DB_SCHEMA=public
# DB_TABLE=dataset_final_gold
```

### 3. Iniciar o Ambiente

Entre no ambiente de desenvolvimento isolado com o Devbox e instale as dependências com Poetry.
Se você for rodar com Docker, pode pular esta etapa e ir direto para “Modos de Execução → Execução com Docker”.

```bash
devbox shell
make install
```

### 4. Executar o Pipeline Completo

Se for executar local (sem Docker), dentro do `devbox shell`, execute:

```bash
make full-run
```

### 5. Validações de Qualidade (GE)

-   Configuração do GE em `great_expectations/great_expectations.yml`.
-   Suítes: `silver_netflix_suite.json` (Silver). Data Docs locais em `great_expectations/uncommitted/data_docs/local_site/` após rodar validação.

---

## Estrutura de Dados (onde os arquivos ficam)

-   `data/` — repositório de dados do pipeline (Medalhão):
    - `data/bronze/` — Parquets brutos gerados na ingestão.
    - `data/silver/` — Parquet consolidado e tratado para ML.
    - `data/gold/` — Parquet final com a coluna `previsao_amount`.
-   `src/scripts/data/` — pode aparecer se o pipeline for executado com diretório de trabalho errado em versões antigas:
    - Antes de padronizarmos caminhos absolutos no orquestrador, alguns alvos do Luigi usavam caminhos relativos e criavam `src/scripts/data`.
    - Já corrigido. Se essa pasta existir, use `make clean` para removê-la; o pipeline oficial usa apenas `data/` na raiz do projeto.
-   `db_data` (volume Docker) — dados internos do PostgreSQL quando você usa `docker-compose`:
    - É um volume nomeado, não aparece como pasta no projeto, mas persiste entre reinicializações.
-   `.db_data/` (apenas se você optar por Postgres local pelo Devbox) — diretório do cluster local do Postgres inicializado pelo Devbox.

## Modos de Execução

### Execução com Docker

-   Variáveis:
    - Se você rodar o pipeline dentro do contêiner `pipeline`, a conexão ao Postgres usa o host `db` automaticamente:
      - `DATABASE_URL=postgresql+psycopg2://postgres:postgres@db:5432/netflixdb`
    - Se você rodar o Python no host, use `localhost`:
      - `DATABASE_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/netflixdb`
-   Comando recomendado:
    - `make run` — sobe o `db` (build se preciso) e executa o pipeline dentro do contêiner `pipeline`.
-   Alternativas:
    - `make run-db` — sobe apenas o Postgres.
    - `make up` / `make down` — sobe/derruba todos os serviços definidos no compose.

### Execução local (sem Docker)

-   Passos:
    - `devbox shell`
    - `make install`
    - `make full-run`
-   Observação:
    - Com esse modo, a conexão padrão do `.env` para `localhost` é a esperada.

### Comandos Úteis (Makefile)

-   `make install` — instala dependências com Poetry.
-   `make run` — sobe os serviços via Docker Compose e executa o pipeline dentro do contêiner `pipeline`.
-   `make clean` — remove a pasta `data/` para uma nova execução.
-   `make full-run` — instala, limpa e roda o pipeline do zero.
-   `make status` — mostra status de contêineres (se estiver usando Docker).
-   `make up` / `make down` / `make reset` — comandos de infraestrutura (opcionais; requerem docker compose).
-   `make run-db` — sobe apenas o Postgres (serviço `db`).



-   Executando localmente no host: `DATABASE_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/netflixdb`
-   Executando dentro de um contêiner do mesmo compose: `...@db:5432/netflixdb`
