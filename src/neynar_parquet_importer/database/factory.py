from typing import Type
from .base import DatabaseBackend
from ..transformers.base import DataTransformer


class DatabaseFactory:
    """Factory for creating appropriate database backend based on settings"""
    
    @staticmethod
    def create_backend(settings) -> DatabaseBackend:
        """Create database backend based on configuration"""
        if settings.database_backend == "postgresql":
            # Wrap existing db.py functions in PostgreSQL backend interface
            from .postgres import PostgreSQLBackend
            return PostgreSQLBackend()
        elif settings.database_backend == "neo4j":
            # Import here to avoid dependency when not needed
            try:
                from .neo4j import Neo4jBackend
                return Neo4jBackend()
            except ImportError as e:
                raise ImportError(
                    f"Neo4j dependencies not available. Install with: pip install neo4j>=5.15.0"
                ) from e
        else:
            raise ValueError(f"Unsupported database backend: {settings.database_backend}")
    
    @staticmethod
    def create_transformer(settings) -> DataTransformer:
        """Create appropriate transformer for the backend"""
        if settings.database_backend == "postgresql":
            from ..transformers.base import PassthroughTransformer
            return PassthroughTransformer()
        elif settings.database_backend == "neo4j":
            # Use memory-efficient streaming transformer for Neo4j
            from ..transformers.streaming import StreamingGraphTransformer
            return StreamingGraphTransformer(chunk_size=getattr(settings, 'transform_chunk_size', 100))
        else:
            raise ValueError(f"No transformer for backend: {settings.database_backend}")
