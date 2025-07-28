#!/usr/bin/env python3
"""
Direct test script for processing real parquet files

This script can be run directly to test parquet file processing
without needing Docker. Useful for development and debugging.

Usage:
    python test_parquet_direct.py [postgresql|neo4j]
"""

import sys
import os
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

from neynar_parquet_importer.settings import Settings
from neynar_parquet_importer.db import get_tables, import_parquet, init_db
from neynar_parquet_importer.progress import ProgressCallback
from neynar_parquet_importer.s3 import parse_parquet_filename
from neynar_parquet_importer.settings import SHUTDOWN_EVENT
from neynar_parquet_importer.context import set_global_settings


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


def process_parquet_file(parquet_file: Path, settings: Settings):
    """Process a single parquet file using the direct import logic"""
    # Initialize global settings
    set_global_settings(settings)
    
    with ExitStack() as stack:
        try:
            parsed_filename = parse_parquet_filename(parquet_file)
            table_name = parsed_filename["table_name"]
            
            print(f"🔄 Processing {parquet_file.name} for table {table_name}")
            
            # Initialize database
            table_names = [table_name]
            if settings.database_backend == "postgresql":
                db_engine = init_db(str(settings.postgres_dsn), table_names, settings)
                tables = get_tables(settings.postgres_schema, db_engine, [])
            else:
                # For Neo4j, we still need PostgreSQL for tracking table
                db_engine = init_db(str(settings.postgres_dsn), table_names, settings)
                tables = get_tables(settings.postgres_schema, db_engine, [])
            
            # Get the table
            try:
                table = tables[table_name]
            except KeyError:
                available_tables = list(tables.keys())
                print(f"❌ Table {table_name} not found in schema {settings.postgres_schema}")
                print(f"Available tables: {available_tables}")
                return None
            
            parquet_import_tracking = tables["parquet_import_tracking"]
            
            # Determine file type
            if parsed_filename["start_timestamp"] > 0:
                file_type = "incremental"
            else:
                file_type = "full"
            
            print(f"📋 File type: {file_type}")
            
            # Set up progress callbacks (disabled for tests)
            # Create a mock progress object since ProgressCallback requires it
            class MockProgress:
                def add_task(self, task_name, total):
                    return 0
                def update(self, task_id, advance):
                    pass
            
            mock_progress = MockProgress()
            progress_callback = ProgressCallback(mock_progress, "files", 0, enabled=False)
            empty_callback = ProgressCallback(mock_progress, "empty", 0, enabled=False)
            
            # Set up executors
            row_group_executor = stack.enter_context(
                ThreadPoolExecutor(
                    max_workers=settings.row_workers,
                    thread_name_prefix=f"{table_name}Rows",
                )
            )
            shutdown_executor = stack.enter_context(
                ThreadPoolExecutor(
                    max_workers=1,
                    thread_name_prefix="Shutdown",
                )
            )
            
            f_shutdown = shutdown_executor.submit(SHUTDOWN_EVENT.wait)
            
            # No row filters for test
            row_filters = []
            
            # Import the parquet file
            print(f"⚡ Starting import with {settings.database_backend} backend...")
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
            import traceback
            traceback.print_exc()
            return None
            
        finally:
            SHUTDOWN_EVENT.set()


def main():
    """Main function to run parquet processing tests"""
    # Get database backend from command line or default to postgresql
    backend = sys.argv[1] if len(sys.argv) > 1 else "postgresql"
    
    if backend not in ["postgresql", "neo4j"]:
        print("❌ Invalid backend. Use 'postgresql' or 'neo4j'")
        sys.exit(1)
    
    print(f"🚀 Testing parquet processing with {backend} backend")
    
    # Find parquet files
    test_dir = Path(__file__).parent
    data_dir = test_dir / "data"
    parquet_files = [
        data_dir / "nindexer-follows-1750957190-1750957191.parquet",
        data_dir / "nindexer-verifications-1749145661-1749145662.parquet"
    ]
    
    # Filter to existing files
    existing_files = [pf for pf in parquet_files if pf.exists()]
    
    if not existing_files:
        print("❌ No parquet files found in tests/data directory")
        print("Expected files:")
        for pf in parquet_files:
            print(f"  - {pf}")
        sys.exit(1)
    
    print(f"📁 Found {len(existing_files)} parquet files:")
    for pf in existing_files:
        print(f"  - {pf.name}")
    
    # Create settings
    settings = create_test_settings(backend)
    
    # Process each file
    results = []
    for parquet_file in existing_files:
        print(f"\n" + "="*50)
        result = process_parquet_file(parquet_file, settings)
        if result is not None:
            results.append(result)
            if result == "already_imported":
                print(f"✅ {parquet_file.name} was already imported")
            else:
                print(f"✅ Successfully processed {parquet_file.name}")
        else:
            print(f"❌ Failed to process {parquet_file.name}")
    
    # Summary
    print(f"\n" + "="*50)
    print(f"🎉 Processing complete!")
    print(f"✅ Successfully processed: {len(results)}/{len(existing_files)} files")
    print(f"🔧 Backend used: {backend}")
    
    if len(results) != len(existing_files):
        sys.exit(1)


if __name__ == "__main__":
    main()
