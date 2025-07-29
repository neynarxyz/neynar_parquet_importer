from typing import Any, List, Optional
from .base import DatabaseBackend, ImportOperation


class PostgreSQLBackend(DatabaseBackend):
    """PostgreSQL backend that wraps existing db.py functionality"""
    
    def __init__(self):
        self.engine = None
    
    def init_db(self, uri: str, tables: List[str], settings: 'Settings') -> Any:
        """Initialize database connection and apply schema migrations
        
        DELEGATION PATTERN: This wrapper delegates to the legacy db.init_db() to preserve
        backwards compatibility and avoid refactoring battle-tested PostgreSQL logic.
        Enables zero-cost abstraction - new backends get clean interfaces, PostgreSQL keeps optimizations.
        """
        # Import here to avoid circular imports
        from .. import db
        
        # Convert uri to string if it's a PostgresDsn object
        uri_str = str(uri) if hasattr(uri, '__str__') else uri
        
        # Delegate to existing battle-tested PostgreSQL initialization logic
        self.engine = db.init_db(uri_str, tables, settings)
        return self.engine
    
    def import_operations(self, operations: List[ImportOperation]) -> None:
        """Import a batch of database operations with appropriate semantics"""
        # For PostgreSQL, operations should be "upsert" type from PassthroughTransformer
        # This is a placeholder - in practice, this won't be called for PostgreSQL
        # because we use the zero-cost path that bypasses transformation
        raise NotImplementedError(
            "PostgreSQL backend should use the zero-cost path that bypasses transformation"
        )
    
    def check_import_progress(self, table_name: str, file_name: str) -> Optional[int]:
        """Check last imported row group for a file"""
        from .. import db
        return db.get_last_imported_row_group(self.engine, table_name, file_name)
    
    def mark_completed(self, file_names: List[str], table_name: str) -> None:
        """Mark files as completed in tracking system"""
        from .. import db
        db.mark_files_as_completed(self.engine, file_names, table_name)
    
    def close(self) -> None:
        """Clean up database connections"""
        if self.engine:
            self.engine.dispose()
