# Neynar Parquet Importer - Test & Development Makefile
.PHONY: help build test test-neo4j test-all clean dev-env setup-neo4j logs down

# Default target
help:
	@echo "Available targets:"
	@echo "  build        - Build all Docker images"
	@echo "  test         - Run all tests in Docker containers"
	@echo "  test-neo4j   - Run tests with Neo4j backend"
	@echo "  validate     - Quick validation of core components"
	@echo "  dev-env      - Start development environment (PostgreSQL + Neo4j)"
	@echo "  setup-neo4j  - Initialize Neo4j with test data"
	@echo "  populate-neo4j-data - Import real parquet files into Neo4j"
	@echo "  validate-neo4j-data - Validate Neo4j data directly"
	@echo "  logs         - Show logs from running containers"
	@echo "  clean        - Stop and remove all containers and volumes"
	@echo "  down         - Stop all services"

# Docker Compose files
DOCKER_COMPOSE = docker-compose -f docker-compose.test.yml

# Build all images
build:
	@echo "🏗️  Building Docker images..."
	$(DOCKER_COMPOSE) build

# Start development environment
dev-env:
	@echo "🚀 Starting development environment..."
	$(DOCKER_COMPOSE) up --detach --wait postgres neo4j
	@echo "✅ Development environment ready!"
	@echo "   PostgreSQL: localhost:25432"
	@echo "   Neo4j Browser: http://localhost:37474"
	@echo "   Neo4j Bolt: neo4j://localhost:37687"

# Run all tests
test: build dev-env
	@echo "🧪 Running all tests..."
	$(DOCKER_COMPOSE) run --rm test-runner

# Test with Neo4j backend specifically
test-neo4j: build dev-env
	@echo "🧪 Testing Neo4j backend functionality..."
	$(DOCKER_COMPOSE) run --rm -e DATABASE_BACKEND=neo4j test-runner uv run pytest tests/ -v

# Initialize Neo4j with test data
setup-neo4j: dev-env
	@echo "🔧 Setting up Neo4j test environment..."
	$(DOCKER_COMPOSE) run --rm test-runner uv run python -c "\
from src.neynar_parquet_importer.database.neo4j import Neo4jBackend; \
from src.neynar_parquet_importer.settings import Settings; \
settings = Settings(database_backend='neo4j', neo4j_uri='neo4j://neo4j:7687'); \
backend = Neo4jBackend(); \
backend.init_db('neo4j://neo4j:7687', ['fids', 'follows'], settings); \
print('✅ Neo4j initialized successfully')"

# Populate Neo4j with real parquet data  
populate-neo4j-data: dev-env
	@echo "📥 Populating Neo4j with real parquet data..."
	$(DOCKER_COMPOSE) run --rm -e DATABASE_BACKEND=neo4j test-runner uv run pytest tests/test_parquet_direct.py::test_neo4j_data_verification -v
	@echo "✅ Neo4j populated with real data!"

# Validate Neo4j data directly
validate-neo4j-data: dev-env
	@echo "🔍 Quick Neo4j validation..."
	@echo "📊 Node Types and Counts:"
	@$(DOCKER_COMPOSE) exec neo4j cypher-shell -u neo4j -p testpassword "MATCH (n) RETURN labels(n) as node_types, count(n) as count ORDER BY count DESC"
	@echo ""
	@echo "🔗 Relationship Types and Counts:"
	@$(DOCKER_COMPOSE) exec neo4j cypher-shell -u neo4j -p testpassword "MATCH ()-[r]->() RETURN type(r) as relationship_types, count(r) as count ORDER BY count DESC"
	@echo ""
	@echo "✅ Neo4j validation complete!"

# Show logs
logs:
	@echo "📋 Showing logs from running containers..."
	$(DOCKER_COMPOSE) logs -f

# Stop services
down:
	@echo "🛑 Stopping all services..."
	$(DOCKER_COMPOSE) down

# Clean up everything
clean:
	@echo "🧹 Cleaning up containers, volumes, and images..."
	$(DOCKER_COMPOSE) down -v --rmi local
	docker system prune -f

# Development helpers
shell: dev-env
	@echo "🐚 Starting interactive shell in test environment..."
	$(DOCKER_COMPOSE) run --rm test-runner bash

# Quick validation
validate: build dev-env
	@echo "⚡ Quick validation test..."
	$(DOCKER_COMPOSE) run --rm test-runner uv run python -c "\
from src.neynar_parquet_importer.settings import Settings; \
from src.neynar_parquet_importer.database.factory import DatabaseFactory; \
print('✅ Settings loaded'); \
print('✅ PostgreSQL backend:', type(DatabaseFactory.create_backend(Settings(database_backend='postgresql'))).__name__); \
print('✅ Neo4j backend:', type(DatabaseFactory.create_backend(Settings(database_backend='neo4j'))).__name__); \
print('🎉 All core components working!')"
