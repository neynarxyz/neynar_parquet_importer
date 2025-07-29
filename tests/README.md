# Real Parquet File Testing

This directory contains tests for processing real parquet files with the Neynar Parquet Importer.

## Test Files

### Real Parquet Data (in `tests/data/`)
- `nindexer-follows-*.parquet` - Multiple real follows data files from Nindexer (5 files available)
- `nindexer-verifications-1749145661-1749145662.parquet` - Real verifications data from Nindexer

### Test Scripts
- `test_db.py` - Database connection and JSON cleaning functionality tests
- `test_filters.py` - Row filtering logic tests for casts, reactions, and channel members
- `test_neynar_api_client.py` - Neynar API client tests (requires API key)
- `test_parquet_direct.py` - Direct testing script that processes real parquet files

## Running Tests

### Using Makefile (Recommended)

**Run all unit tests with PostgreSQL backend:**
```bash
make test
```

**Run all unit tests with Neo4j backend:**
```bash
make test-neo4j
```

**Validate complete infrastructure from clean state:**
```bash
make validate
```

**Start development environment (databases only):**
```bash
make dev-env
```

**View all available targets:**
```bash
make help
```

### Complete Makefile Reference

```bash
make build           # Build all Docker images
make test            # Run all unit tests with PostgreSQL backend  
make test-neo4j      # Run all unit tests with Neo4j backend
make validate        # Quick validation of core components
make dev-env         # Start development environment (PostgreSQL + Neo4j)
make setup-neo4j     # Initialize Neo4j with test data
make validate-neo4j-data # Validate Neo4j data directly
make logs            # Show logs from running containers
make clean           # Stop and remove all containers and volumes (frees ~21GB)
make down            # Stop all services
make shell           # Interactive shell in test environment
```

### Using Direct Script

The direct script processes real parquet files using Docker containers:

```bash
# Test with PostgreSQL backend
docker-compose -f docker-compose.yml -f docker-compose.test.yml run --rm test-runner uv run python tests/test_parquet_direct.py postgresql

# Test with Neo4j backend  
docker-compose -f docker-compose.yml -f docker-compose.test.yml run --rm test-runner uv run python tests/test_parquet_direct.py neo4j
```

### Using Pytest Directly

```bash
# Run all unit tests with PostgreSQL backend
docker-compose -f docker-compose.yml -f docker-compose.test.yml run --rm test-runner pytest tests/ -v

# Run all unit tests with Neo4j backend (using make target)
make test-neo4j

# Run specific test files
docker-compose -f docker-compose.yml -f docker-compose.test.yml run --rm test-runner pytest tests/test_db.py -v
docker-compose -f docker-compose.yml -f docker-compose.test.yml run --rm test-runner pytest tests/test_filters.py -v
```

## Database Requirements

### PostgreSQL (Docker)
- Host: postgres (Docker service name)
- Port: 5432 (internal Docker port, exposed on both 5432 and 25432 for compatibility)
- User: postgres
- Password: postgres
- Database: neynar_parquet_importer

### Neo4j (Docker)
- URI: neo4j://neo4j:7687 (Docker service name, exposed on 37687)
- User: neo4j
- Password: testpassword
- Database: neo4j

**Note**: When running inside Docker containers (recommended), the services communicate via internal Docker network using the service names and internal ports.

## Environment Variables

For Docker-based testing (recommended), the defaults work out of the box. You can customize database connections using these environment variables in your `.env` file:

```bash
# PostgreSQL (for Docker containers)
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=neynar_parquet_importer

# Neo4j (for Docker containers)
NEO4J_URI=neo4j://neo4j:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=testpassword
```

## What the Tests Do

### Unit Tests
The pytest-based unit tests validate core functionality:

1. **test_db.py**: Tests JSON cleaning functions (`clean_embeds`)
2. **test_filters.py**: Tests row filtering logic for casts, reactions, and channel members
3. **test_neynar_api_client.py**: Tests Neynar API client (requires `NEYNAR_API_KEY`)

### Real Parquet Processing Tests
The `test_parquet_direct.py` script processes real parquet files using the same logic as the production import pipeline:

1. **Parse parquet filename** to extract table information
2. **Initialize database** and apply schema migrations
3. **Set up executors** for parallel processing  
4. **Import parquet data** using the existing `import_parquet` function
5. **Track imports** to prevent duplicate processing
6. **Verify results** and report success/failure

This ensures that:
- The parquet parsing logic works correctly
- Database schema creation and migrations work
- Data transformation and import logic works
- Both PostgreSQL and Neo4j backends function properly
- Real-world Farcaster data can be processed successfully
- Import tracking prevents duplicate processing

## Troubleshooting

**Database connection errors:**
- Ensure databases are running: `make dev-env`
- Check Docker services: `docker-compose -f docker-compose.yml -f docker-compose.test.yml ps`
- Verify containers are healthy (should show "healthy" status)

**Unit test failures:**
- **API test failures**: Expected if `NEYNAR_API_KEY` is not set
- **Connection failures**: Ensure databases are running and healthy
- **Missing dependencies**: Run `make dev-env` to start all services

**Real parquet processing issues:**
- **"Already imported" messages**: Normal behavior - files are tracked to prevent duplicate imports
- **Schema errors**: Database migrations should auto-apply
- **Permission errors**: Ensure Docker has proper permissions

**Performance:**
- First run may be slower due to Docker image pulls and compilation
- Subsequent runs are much faster due to caching

## Expected Test Results

### Unit Tests
When running `make test` or `make test-neo4j`, expect:
- ✅ **4/5 tests passing** (this is normal)
- ✅ `test_clean_embeds` - JSON cleaning functionality
- ✅ `test_filter_casts` - Row filtering for casts  
- ✅ `test_filter_reactions` - Row filtering for reactions
- ✅ `test_filter_channel_members` - Row filtering for channel members
- ❌ `test_get_portal_pricing` - **Expected failure** (requires `NEYNAR_API_KEY`)

### Real Parquet Processing
When running the direct parquet tests:
- ✅ **Database migrations succeed** (tables created automatically)
- ✅ **Files processed successfully** (may show "already imported" on repeat runs)
- ✅ **Both backends work** (PostgreSQL and Neo4j process same data correctly)
- ℹ️  **"Already imported" messages are normal** - prevents duplicate data
