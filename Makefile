# Makefile para o Projeto de Pipeline de Dados

# Define o Poetry para ser usado nos comandos
POETRY = poetry

# ==============================================================================
# Comandos do Ambiente Docker
# ==============================================================================

# Para usar depois de ja ter instalado tudo. Roda sem o build.
.PHONY: up
up:
	@echo "🚀 Subindo todos os serviços com Docker Compose..."
	@docker-compose up -d
#	@poetry run python src/scripts/main.py EtapaGold --local-scheduler

.PHONY: down
down:
	@echo "🛑 Parando todos os serviços..."
	@docker-compose down

.PHONY: reset
reset:
	@echo "🔄 Resetando o ambiente Docker (parando e removendo volumes)..."
	@docker-compose down --volumes
	@echo "✅ Ambiente Docker limpo."
	@make up

.PHONY: status
status:
	@echo "📊 Status atual dos contêineres:"
	@docker-compose ps

# ==============================================================================
# Comandos do Pipeline
# ==============================================================================

.PHONY: install
install:
	@echo "📦 Instalando dependências Python com Poetry..."
	@$(POETRY) lock
	@$(POETRY) install

.PHONY: clean
clean:
	@echo "🧹 Limpando dados gerados pelo pipeline..."
	@rm -rf data/ src/scripts/data/

.PHONY: prune
prune:
	@echo "🧽 Removendo infra desnecessária/duplicada e caches locais..."
	@rm -rf gx/ pipeline/great_expectations/ .venv/ .devbox/ .db_data/
	@echo "✅ Pastas removidas com segurança. (great_expectations/ mantida)"

.PHONY: run
run:
	@echo "▶️ Executando o pipeline dentro do contêiner..."
	@docker-compose up --build -d
	@docker-compose exec pipeline python src/scripts/main.py EtapaGold --local-scheduler

.PHONY: project-init
project-init:
	@echo "🚀 Iniciando o projeto completo..."
	@$(MAKE) full-run

.PHONY: full-run
full-run: install clean run
	@echo "✅ Pipeline executado do zero com sucesso!"

.PHONY: run-db
run-db:
	@docker-compose up --build -d db
	@echo "✅ Postgres disponível no 5432."

# ==============================================================================
# Comandos de Ajuda
# ==============================================================================

.PHONY: help
help:
	@echo "Comandos disponíveis:"
	@echo "  make project-init - Executa o pipeline completo (instala, limpa e roda)."
	@echo "  make install    - Instala as dependências do projeto."
	@echo "  make up         - Sobe os contêineres Docker em background."
	@echo "  make down       - Para os contêineres Docker."
	@echo "  make reset      - Para, remove os volumes e sobe os contêineres novamente."
	@echo "  make status     - Mostra o status dos contêineres."
	@echo "  make clean      - Apaga os dados gerados para forçar uma nova execução."
	@echo "  make run        - Executa o pipeline via main.py."
	@echo "  make full-run   - Limpa os dados e executa o pipeline do zero."
