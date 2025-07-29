from abc import ABC, abstractmethod
from typing import Dict, List
from ..database.base import ImportOperation


class DataTransformer(ABC):
    """Base class for transforming parquet table rows into database operations"""
    
    @abstractmethod
    def transform_table(self, table_name: str, rows: List[Dict]) -> List[ImportOperation]:
        """Transform rows from a specific parquet table into database operations"""
        pass


class ParquetTableTransformer(ABC):
    """Base for table-specific transformers that understand parquet schema"""
    
    @abstractmethod
    def supports_table(self, table_name: str) -> bool:
        """Whether this transformer can handle the given parquet table"""
        pass
    
    @abstractmethod
    def transform_rows(self, rows: List[Dict]) -> List[ImportOperation]:
        """Transform rows from THIS table type into operations"""
        pass


class PassthroughTransformer(DataTransformer):
    """Default transformer that passes data unchanged (PostgreSQL mode)"""
    
    def transform_table(self, table_name: str, rows: List[Dict]) -> List[ImportOperation]:
        """Convert rows to simple upsert operations - zero transformation"""
        operations = []
        for row in rows:
            operations.append(ImportOperation(
                operation_type="upsert",
                entity_type=table_name,
                properties=row
            ))
        return operations


class GraphTransformer(DataTransformer):
    """Transformer for graph databases that uses table-specific transformers"""
    
    def __init__(self):
        self.table_transformers = {}
        self._register_table_transformers()
    
    def transform_table(self, table_name: str, rows: List[Dict]) -> List[ImportOperation]:
        """Route to appropriate table-specific transformer"""
        transformer = self.table_transformers.get(table_name)
        if transformer:
            return transformer.transform_rows(rows)
        else:
            # Default: create nodes with table name as label
            return self._default_node_creation(table_name, rows)
    
    def _register_table_transformers(self):
        """Register table-specific transformers dynamically based on available tables"""
        # Import transformers (lazy import to avoid dependency issues)
        try:
            from ..transformers.follows import FollowsGraphTransformer
            from ..transformers.users import UsersGraphTransformer
            from ..transformers.verifications import VerificationsGraphTransformer
            
            # Define transformer mappings - can be extended dynamically
            transformer_map = {
                # Social graph transformers
                'follows': FollowsGraphTransformer(),
                'nindexer_follows': FollowsGraphTransformer(),
                
                # User transformers
                'fids': UsersGraphTransformer(),
                'nindexer_fids': UsersGraphTransformer(),
                'profiles': UsersGraphTransformer(),
                'nindexer_profiles': UsersGraphTransformer(),
                
                # Verification transformers
                'verifications': VerificationsGraphTransformer(),
                'account_verifications': VerificationsGraphTransformer(),
                'nindexer_verifications': VerificationsGraphTransformer(),
            }
            
            self.table_transformers = transformer_map
        except ImportError:
            # If specific transformers aren't available, use empty dict
            # This allows the system to still work with default node creation
            self.table_transformers = {}
    
    def _default_node_creation(self, table_name: str, rows: List[Dict]) -> List[ImportOperation]:
        """Default fallback: create nodes with table name as label"""
        operations = []
        for row in rows:
            operations.append(ImportOperation(
                operation_type="create_node",
                entity_type=table_name.title(),  # Convert table name to node label
                properties=row
            ))
        return operations
