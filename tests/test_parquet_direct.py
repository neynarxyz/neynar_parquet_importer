#!/usr/bin/env python3
"""
Pytest tests for processing real parquet files

These tests verify that parquet file processing works correctly
with both PostgreSQL and Neo4j backends using real data files.
"""

import sys
import os
from pathlib import Path
import pytest
import signal

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack

# Set up logging with more verbose output for debugging
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more detailed output
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Enable debug logging for specific Neo4j components
neo4j_logger = logging.getLogger('neynar_parquet_importer.database.neo4j')
neo4j_logger.setLevel(logging.DEBUG)

# Enable debug logging for database operations
db_logger = logging.getLogger('neynar_parquet_importer.db')
db_logger.setLevel(logging.DEBUG)

from neynar_parquet_importer.settings import Settings
from neynar_parquet_importer.db import get_tables, import_parquet, init_db
from neynar_parquet_importer.progress import ProgressCallback
from neynar_parquet_importer.s3 import parse_parquet_filename
from neynar_parquet_importer.settings import SHUTDOWN_EVENT

@pytest.fixture(autouse=True)
def setup_shutdown_event():
    """Ensure shutdown event is cleared before each test"""
    SHUTDOWN_EVENT.clear()
    yield
    # Don't set it after, let each test manage its own state


def create_test_settings(database_backend: str = "postgresql") -> Settings:
    """Create test settings with appropriate database configuration"""
    # Use environment variables or defaults for test database
    postgres_host = os.getenv("POSTGRES_HOST", "postgres")  # Docker service name  
    postgres_port = os.getenv("POSTGRES_PORT", "5432")  # Internal Docker port
    postgres_user = os.getenv("POSTGRES_USER", "postgres")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "postgres")
    postgres_db = os.getenv("POSTGRES_DB", "neynar_parquet_importer")
    
    neo4j_uri = os.getenv("NEO4J_URI", "neo4j://neo4j:7687")  # Docker service name
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD", "testpassword")
    
    # Construct the PostgreSQL DSN for Docker environment
    postgres_dsn = f"postgresql+psycopg://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"
    
    settings = Settings(
        database_backend=database_backend,
        postgres_dsn=postgres_dsn,
        postgres_schema="public",
        postgres_pool_size=10,
        postgres_max_overflow=5,
        postgres_connection_timeout=30,
        neo4j_uri=neo4j_uri,
        neo4j_user=neo4j_user,
        neo4j_password=neo4j_password,
        parquet_s3_database="public-postgres",
        parquet_s3_schema="nindexer",
        npe_version="v3",
        incremental_duration=1,
        row_workers=2,
        file_workers=1,
        download_workers=1,
        datadog_enabled=False,
        log_level="INFO",
        log_format="standard"
    )
    
    return settings


@pytest.fixture
def test_parquet_files():
    """Get list of available test parquet files"""
    test_dir = Path(__file__).parent
    data_dir = test_dir / "data"
    parquet_files = [
        data_dir / "nindexer-follows-1750957190-1750957191.parquet",
        data_dir / "nindexer-verifications-1749145661-1749145662.parquet"
    ]
    
    # Filter to existing files
    existing_files = [pf for pf in parquet_files if pf.exists()]
    
    if not existing_files:
        pytest.skip("No parquet test files found in tests/data directory")
    
    return existing_files


def process_parquet_file(parquet_file: Path, settings: Settings):
    """Process a single parquet file using the direct import logic"""
    
    # Clear shutdown event before starting
    SHUTDOWN_EVENT.clear()
    print(f"🔧 Cleared shutdown event")
    
    with ExitStack() as stack:
        try:
            print(f"📝 Parsing filename: {parquet_file.name}")
            parsed_filename = parse_parquet_filename(parquet_file)
            table_name = parsed_filename["table_name"]
            
            print(f"🔄 Processing {parquet_file.name} for table {table_name}")
            
            # Initialize database
            print(f"🗄️  Initializing database connection...")
            table_names = [table_name]
            if settings.database_backend == "postgresql":
                print(f"🐘 Connecting to PostgreSQL...")
                db_engine = init_db(str(settings.postgres_dsn), table_names, settings)
                print(f"🔍 Getting PostgreSQL tables...")
                tables = get_tables(settings.postgres_schema, db_engine, [])
            else:
                print(f"🟢 Using Neo4j backend (PostgreSQL for tracking)...")
                # For Neo4j, we still need PostgreSQL for tracking table
                db_engine = init_db(str(settings.postgres_dsn), table_names, settings)
                tables = get_tables(settings.postgres_schema, db_engine, [])
            
            print(f"✅ Database connection established, found {len(tables)} tables")
            
            # Get the table
            try:
                table = tables[table_name]
                print(f"📋 Found target table: {table_name}")
            except KeyError:
                available_tables = list(tables.keys())
                print(f"❌ Table {table_name} not found in schema {settings.postgres_schema}")
                print(f"Available tables: {available_tables}")
                return None
            
            parquet_import_tracking = tables["parquet_import_tracking"]
            print(f"📊 Found tracking table")
            
            # Determine file type
            if parsed_filename["start_timestamp"] > 0:
                file_type = "incremental"
            else:
                file_type = "full"
            
            print(f"📋 File type: {file_type}")
            
            # Set up progress callbacks (disabled for tests)
            print(f"⚙️  Setting up progress callbacks...")
            # Create a mock progress object since ProgressCallback requires it
            class MockProgress:
                def add_task(self, task_name, total):
                    return 0
                def update(self, task_id, advance):
                    pass
            
            mock_progress = MockProgress()
            progress_callback = ProgressCallback(mock_progress, "files", 0, enabled=False)
            empty_callback = ProgressCallback(mock_progress, "empty", 0, enabled=False)
            print(f"✅ Progress callbacks ready")
            
            # Set up executors with reduced worker count for tests
            print(f"🔧 Setting up thread pool executors...")
            row_group_executor = stack.enter_context(
                ThreadPoolExecutor(
                    max_workers=1,  # Reduced from settings.row_workers
                    thread_name_prefix=f"{table_name}Rows",
                )
            )
            shutdown_executor = stack.enter_context(
                ThreadPoolExecutor(
                    max_workers=1,
                    thread_name_prefix="Shutdown",
                )
            )
            print(f"✅ Thread pool executors ready")
            
            print(f"⏰ Submitting shutdown event waiter...")
            f_shutdown = shutdown_executor.submit(SHUTDOWN_EVENT.wait)
            print(f"✅ Shutdown event waiter submitted")
            
            # No row filters for test
            row_filters = []
            print(f"🚫 No row filters applied for test")
            
            # Import the parquet file with timeout
            print(f"⚡ Starting import with {settings.database_backend} backend...")
            
            # Set up a timeout for the import operation
            import signal
            import time
            
            def import_timeout_handler(signum, frame):
                print(f"⏰ TIMEOUT: Import operation timed out after 25 seconds!")
                raise TimeoutError("Import operation timed out after 25 seconds")
            
            signal.signal(signal.SIGALRM, import_timeout_handler)
            signal.alarm(25)  # 25 second timeout for import
            print(f"⏰ Set 25-second timeout for import operation")
            
            try:
                print(f"🚀 Calling import_parquet function...")
                print(f"   📁 File: {parquet_file}")
                print(f"   📋 Table: {table}")
                print(f"   🏷️  File type: {file_type}")
                print(f"   🔧 Backend: {settings.database_backend}")
                
                result = import_parquet(
                    db_engine,
                    table,
                    parquet_file,
                    file_type,
                    progress_callback,
                    empty_callback,
                    parquet_import_tracking,
                    row_group_executor,
                    row_filters,
                    settings,
                    f_shutdown,
                    backfill_start_timestamp=None,
                    backfill_end_timestamp=None,
                )
                print(f"🎉 import_parquet function returned successfully!")
            finally:
                signal.alarm(0)  # Cancel the timeout
                print(f"✅ Timeout cancelled")
            
            print(f"✅ Import completed! Result: {result}")
            
            # Handle case where file was already imported (result is None)
            if result is None:
                print(f"ℹ️  File {parquet_file.name} was already imported (skipped)")
                return "already_imported"
            else:
                print(f"🎯 Successfully imported {parquet_file.name}")
                return result
            
        except Exception as e:
            print(f"❌ Error processing {parquet_file.name}: {e}")
            print(f"🔍 Exception type: {type(e).__name__}")
            import traceback
            traceback.print_exc()
            return None
            
        finally:
            print(f"🧹 Cleaning up process_parquet_file...")
            
            # Explicitly shutdown executors to prevent hanging
            try:
                print(f"🛑 Shutting down executors...")
                if 'row_group_executor' in locals():
                    print(f"🔄 Shutting down row_group_executor...")
                    row_group_executor.shutdown(wait=False)
                    print(f"✅ row_group_executor shutdown initiated")
                
                if 'shutdown_executor' in locals():
                    print(f"🔄 Shutting down shutdown_executor...")
                    shutdown_executor.shutdown(wait=False)
                    print(f"✅ shutdown_executor shutdown initiated")
                    
                print(f"🔄 Setting shutdown event to stop waiting threads...")
                SHUTDOWN_EVENT.set()
                print(f"✅ Shutdown event set")
                
            except Exception as cleanup_error:
                print(f"⚠️  Error during executor cleanup: {cleanup_error}")
            
            print(f"🔄 About to exit ExitStack context manager...")
            # Don't set shutdown event here in tests to avoid affecting other tests
            pass


@pytest.mark.parametrize("backend", ["postgresql", "neo4j"])
def test_parquet_processing(backend, test_parquet_files):
    """Test parquet file processing with different database backends"""
    import signal
    
    print(f"\n🧪 Starting test_parquet_processing with backend: {backend}")
    
    def timeout_handler(signum, frame):
        print(f"⏰ TEST TIMEOUT: Test timed out after 30 seconds for backend {backend}")
        raise TimeoutError("Test timed out after 30 seconds")
    
    # Set a 30-second timeout for the test
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(30)
    print(f"⏰ Set 30-second timeout for test")
    
    try:
        # Get database backend from environment or use the parameterized value
        database_backend = os.getenv("DATABASE_BACKEND", backend)
        
        print(f"🚀 Testing parquet processing with {database_backend} backend")
        print(f"🔧 Test files available: {[f.name for f in test_parquet_files]}")
        
        # Create settings for the specified backend
        print(f"⚙️  Creating settings for {database_backend} backend...")
        settings = create_test_settings(database_backend)
        print(f"✅ Settings created successfully")
        
        # Use the smallest test file to minimize processing time
        parquet_file = min(test_parquet_files, key=lambda f: f.stat().st_size)
        print(f"\n" + "="*50)
        print(f"Processing smallest test file: {parquet_file.name} ({parquet_file.stat().st_size} bytes)")
        
        print(f"🔄 About to call process_parquet_file...")
        result = process_parquet_file(parquet_file, settings)
        print(f"✅ process_parquet_file returned: {result}")
        
        # Assert that processing was successful (either imported or already imported)
        assert result is not None, f"Failed to process {parquet_file.name}"
        
        if result == "already_imported":
            print(f"✅ {parquet_file.name} was already imported")
        else:
            print(f"✅ Successfully processed {parquet_file.name}")
        
        print(f"\n" + "="*50)
        print(f"🎉 Processing complete!")
        print(f"✅ Successfully processed: {parquet_file.name}")
        print(f"🔧 Backend used: {database_backend}")
        
    except TimeoutError as e:
        print(f"⏰ TIMEOUT ERROR: {e}")
        pytest.fail(f"Test timed out - parquet processing took longer than 30 seconds with {backend} backend")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: {e}")
        print(f"🔍 Exception type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        # Cancel the alarm
        signal.alarm(0)
        print(f"🧹 Test cleanup completed for {backend} backend")


def test_postgresql_backend(test_parquet_files):
    """Test parquet processing specifically with PostgreSQL backend"""
    settings = create_test_settings("postgresql")
    
    # Process at least one file to verify PostgreSQL backend works
    parquet_file = test_parquet_files[0]
    result = process_parquet_file(parquet_file, settings)
    
    assert result is not None, f"PostgreSQL backend failed to process {parquet_file.name}"
    assert result in ["already_imported"] or isinstance(result, dict), f"Unexpected result type: {type(result)}"


def test_neo4j_backend(test_parquet_files):
    """Test parquet processing specifically with Neo4j backend"""
    settings = create_test_settings("neo4j")
    
    # Process at least one file to verify Neo4j backend works
    parquet_file = test_parquet_files[0]
    result = process_parquet_file(parquet_file, settings)
    
    assert result is not None, f"Neo4j backend failed to process {parquet_file.name}"
    assert result in ["already_imported"] or isinstance(result, dict), f"Unexpected result type: {type(result)}"


def test_neo4j_data_verification():
    """Verify that Neo4j backend creates actual nodes and relationships
        TODO: This test should tightly integrate the full neo4j db lifecycle 
    """
    import neo4j
    from sqlalchemy import text
    
    print(f"\n🔍 Verifying Neo4j data and relationships...")
    
    settings = create_test_settings("neo4j")
    
    # Connect to Neo4j to verify data exists
    neo4j_driver = neo4j.GraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password)
    )
    
    try:
        neo4j_driver.verify_connectivity()
        print(f"✅ Connected to Neo4j")
        
        # Clear existing data and tracking for a clean test
        with neo4j_driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            
        # Process a small follows file first to get filename
        test_dir = Path(__file__).parent
        data_dir = test_dir / "data"
        follows_files = list(data_dir.glob("*follows*.parquet"))
        
        if not follows_files:
            pytest.skip("No follows parquet files found for testing")
        
        # Use smallest follows file
        parquet_file = min(follows_files, key=lambda f: f.stat().st_size)
        print(f"📁 Processing: {parquet_file.name}")
        
        # Clear tracking for this specific file only (force re-import)
        from neynar_parquet_importer.db import init_db
        from neynar_parquet_importer.s3 import parse_parquet_filename
        
        # Parse the filename to get the table name
        parsed_filename = parse_parquet_filename(parquet_file)
        table_name = parsed_filename["table_name"]
        
        pg_engine = init_db(str(settings.postgres_dsn), [table_name], settings)
        with pg_engine.connect() as conn:
            # Delete any existing tracking for this specific file to force re-import
            # Try both the filename and absolute path since we're not sure which is stored
            result = conn.execute(text("""
                DELETE FROM parquet_import_tracking 
                WHERE file_name = :file_name OR file_name = :file_path
            """), {
                "file_name": parquet_file.name,
                "file_path": str(parquet_file)
            })
            rows_deleted = result.rowcount
            conn.commit()
            print(f"🧹 Deleted {rows_deleted} tracking record(s) for file: {parquet_file.name}")
        
        print(f"🧹 Cleared existing data and tracking for file: {parquet_file.name}")
        
        # Process the file
        result = process_parquet_file(parquet_file, settings)
        
        # Verify Neo4j contains the expected data
        with neo4j_driver.session() as session:
            # Check User nodes
            user_result = session.run("MATCH (u:User) RETURN count(u) as count")
            user_count = user_result.single()["count"]
            
            # Check FOLLOWS relationships
            follows_result = session.run("MATCH ()-[r:FOLLOWS]->() RETURN count(r) as count")
            follows_count = follows_result.single()["count"]
            
            # Get a sample relationship
            sample_result = session.run("""
                MATCH (u:User)-[r:FOLLOWS]->(v:User) 
                RETURN u.fid as from_fid, v.fid as to_fid 
                LIMIT 1
            """)
            sample = sample_result.single()
            
            print(f"📊 Neo4j Results:")
            print(f"   👥 User nodes: {user_count}")
            print(f"   🔗 FOLLOWS relationships: {follows_count}")
            if sample:
                print(f"   📋 Sample: FID {sample['from_fid']} → FID {sample['to_fid']}")
            
            # Assertions
            assert user_count >= 2, f"Expected at least 2 User nodes, found {user_count}"
            assert follows_count >= 1, f"Expected at least 1 FOLLOWS relationship, found {follows_count}"
            assert sample is not None, "Expected at least one relationship sample"
            
            print(f"✅ Neo4j backend successfully created nodes and relationships!")
        
    finally:
        if neo4j_driver:
            neo4j_driver.close()


def test_neo4j_relationships_explicitly():
    """Test that Neo4j backend actually creates nodes and relationships"""
    import neo4j
    from neynar_parquet_importer.s3 import parse_parquet_filename
    from sqlalchemy import text
    
    print(f"\n🧪 Starting explicit Neo4j relationship test...")
    
    settings = create_test_settings("neo4j")
    
    # Clear shutdown event
    SHUTDOWN_EVENT.clear()
    print(f"🔧 Cleared shutdown event")
    
    # Connect to Neo4j directly to verify data
    neo4j_driver = neo4j.GraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password)
    )
    
    try:
        # Test connectivity
        neo4j_driver.verify_connectivity()
        print(f"✅ Connected to Neo4j at {settings.neo4j_uri}")
        
        # Get initial counts before processing
        with neo4j_driver.session() as session:
            # Count existing User nodes
            result = session.run("MATCH (u:User) RETURN count(u) as count")
            initial_user_count = result.single()["count"]
            print(f"📊 Initial User nodes: {initial_user_count}")
            
            # Count existing FOLLOWS relationships  
            result = session.run("MATCH ()-[r:FOLLOWS]->() RETURN count(r) as count")
            initial_follows_count = result.single()["count"]
            print(f"📊 Initial FOLLOWS relationships: {initial_follows_count}")
            
            # Clear any existing data for clean test
            print(f"🧹 Clearing existing test data...")
            session.run("MATCH (n) DETACH DELETE n")  # Clear all nodes and relationships
            
            # Verify clean state
            result = session.run("MATCH (n) RETURN count(n) as count")
            clean_count = result.single()["count"]
            print(f"✅ After cleanup - Total nodes: {clean_count}")
        
        # Find the smallest parquet file to minimize test time
        test_dir = Path(__file__).parent
        data_dir = test_dir / "data"
        parquet_files = list(data_dir.glob("*.parquet"))
        
        if not parquet_files:
            pytest.skip("No parquet test files found")
        
        # Use the smallest file
        parquet_file = min(parquet_files, key=lambda f: f.stat().st_size)
        print(f"📁 Using test file: {parquet_file.name} ({parquet_file.stat().st_size} bytes)")
        
        # Also clear PostgreSQL tracking table for this specific file to force re-import
        print(f"🧹 Clearing PostgreSQL import tracking for file: {parquet_file.name}")
        from neynar_parquet_importer.db import init_db, get_tables
        
        # Connect to PostgreSQL to clear tracking for this specific file
        from neynar_parquet_importer.db import init_db, get_tables
        from neynar_parquet_importer.s3 import parse_parquet_filename
        
        # Parse the filename to get the table name
        parsed_filename = parse_parquet_filename(parquet_file)
        table_name = parsed_filename["table_name"]
        
        pg_engine = init_db(str(settings.postgres_dsn), [table_name], settings)
        with pg_engine.connect() as conn:
            # Delete any existing tracking for this specific file to force re-import
            # Try both the filename and absolute path since we're not sure which is stored
            result = conn.execute(text("""
                DELETE FROM parquet_import_tracking 
                WHERE file_name = :file_name OR file_name = :file_path
            """), {
                "file_name": parquet_file.name,
                "file_path": str(parquet_file)
            })
            rows_deleted = result.rowcount
            conn.commit()
            print(f"✅ Deleted {rows_deleted} tracking record(s) for file: {parquet_file.name}")
        
        # Process the file with enhanced settings for debugging
        print(f"🚀 Processing parquet file with Neo4j backend...")
        
        # Enable more verbose logging for the database backend
        import logging
        backend_logger = logging.getLogger('neynar_parquet_importer.database')
        backend_logger.setLevel(logging.DEBUG)
        
        result = process_parquet_file(parquet_file, settings)
        print(f"✅ Processing result: {result}")
        
        # Verify data was actually inserted into Neo4j
        print(f"🔍 Verifying Neo4j data insertion...")
        with neo4j_driver.session() as session:
            # Count all nodes
            result = session.run("MATCH (n) RETURN count(n) as count")
            total_nodes = result.single()["count"]
            print(f"📊 Total nodes: {total_nodes}")
            
            # Count User nodes
            result = session.run("MATCH (u:User) RETURN count(u) as count")
            final_user_count = result.single()["count"]
            print(f"📊 User nodes: {final_user_count}")
            
            # Count all relationships
            result = session.run("MATCH ()-[r]->() RETURN count(r) as count")
            total_relationships = result.single()["count"]
            print(f"📊 Total relationships: {total_relationships}")
            
            # Count FOLLOWS relationships
            result = session.run("MATCH ()-[r:FOLLOWS]->() RETURN count(r) as count")
            final_follows_count = result.single()["count"]
            print(f"📊 FOLLOWS relationships: {final_follows_count}")
            
            # List all node labels to see what was created
            result = session.run("CALL db.labels()")
            labels = [record['label'] for record in result]
            print(f"📋 Node labels in database: {labels}")
            
            # List all relationship types
            result = session.run("CALL db.relationshipTypes()")
            rel_types = [record['relationshipType'] for record in result]
            print(f"📋 Relationship types in database: {rel_types}")
            
            # Get sample data to verify structure (flexible query to avoid property issues)
            if final_follows_count > 0:
                result = session.run("""
                    MATCH (u)-[r:FOLLOWS]->(v) 
                    RETURN u, v, r
                    LIMIT 5
                """)
                
                sample_relationships = []
                for record in result:
                    u_props = dict(record['u'])
                    v_props = dict(record['v'])
                    r_props = dict(record['r'])
                    sample_relationships.append({
                        'source': u_props,
                        'target': v_props,
                        'relationship': r_props
                    })
                
                print(f"📋 Sample relationships found: {len(sample_relationships)}")
                for i, rel in enumerate(sample_relationships):
                    print(f"   {i+1}. Source: {rel['source']} -> Target: {rel['target']}")
                    print(f"       Relationship: {rel['relationship']}")
            
            # Also check for any import tracking nodes
            result = session.run("MATCH (t:ImportTracking) RETURN count(t) as count")
            tracking_count = result.single()["count"]
            print(f"📊 ImportTracking nodes: {tracking_count}")
            
            # Assertions based on what we actually found
            print(f"🎯 Verification Results:")
            print(f"   � Total nodes created: {total_nodes}")
            print(f"   📈 User nodes created: {final_user_count}")
            print(f"   🔗 Total relationships created: {total_relationships}")
            print(f"   🔗 FOLLOWS relationships created: {final_follows_count}")
            
            if result != "already_imported":
                # File should have been processed, expect some data
                assert total_nodes > 0, f"Expected nodes to be created, but found {total_nodes}"
                assert total_relationships > 0, f"Expected relationships to be created, but found {total_relationships}"
                print(f"✅ Data successfully imported into Neo4j!")
            else:
                print(f"ℹ️  File was already imported, this test needs to be run with clean state")
                # Even if already imported, we should see the structure is correct
                if total_nodes > 0:
                    print(f"✅ Found existing data structure in Neo4j")
                else:
                    print(f"❌ No data found in Neo4j - might be a configuration issue")
        
        print(f"✅ Neo4j relationship test completed!")
        
    finally:
        # Clean up
        if neo4j_driver:
            neo4j_driver.close()
            print(f"🔒 Neo4j connection closed")

# Legacy main function for backward compatibility (when run as script)
def main():
    """Legacy main function - now runs pytest instead"""
    import subprocess
    import sys
    
    # Get database backend from command line or default to postgresql
    backend = sys.argv[1] if len(sys.argv) > 1 else "postgresql"
    
    if backend not in ["postgresql", "neo4j"]:
        print("❌ Invalid backend. Use 'postgresql' or 'neo4j'")
        sys.exit(1)
    
    print(f"🚀 Running pytest tests with {backend} backend")
    
    # Run pytest on this file with the specified backend
    test_file = __file__
    pytest_args = [
        "-v", 
        test_file,
        "-k", f"test_{backend}_backend"
    ]
    
    result = subprocess.run(["python", "-m", "pytest"] + pytest_args)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
