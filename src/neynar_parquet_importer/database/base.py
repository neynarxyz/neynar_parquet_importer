from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
from dataclasses import dataclass

@dataclass
class ImportOperation:
    """Represents a single database operation (node creation, relationship, etc.)"""
    operation_type: str  # "create_node", "create_relationship", "upsert", etc.
    entity_type: str     # "User", "Cast", "FOLLOWS", etc.
    properties: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None  # Additional context for the operation


class DatabaseBackend(ABC):
    """Abstract base class for database backends (PostgreSQL, Neo4j, etc.)"""
    
    @abstractmethod
    def init_db(self, uri: str, tables: List[str], settings: 'Settings') -> Any:
        """Initialize database connection and apply schema migrations"""
        pass
    
    @abstractmethod
    def import_operations(self, operations: List[ImportOperation]) -> None:
        """Import a batch of database operations with appropriate semantics"""
        pass
    
    @abstractmethod
    def check_import_progress(self, table_name: str, file_name: str) -> Optional[int]:
        """Check last imported row group for a file"""
        pass
    
    @abstractmethod
    def mark_completed(self, file_names: List[str], table_name: str) -> None:
        """Mark files as completed in tracking system"""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Clean up database connections"""
        pass
