"""
Memory-efficient streaming transformers that avoid data amplification.

Instead of creating multiple copies of data, these transformers process
rows in small chunks and yield operations as generators.
"""
from typing import Dict, List, Iterator, Optional
from ..database.base import ImportOperation
from ..database.unified_performance import UnifiedPerformanceManager
from .base import GraphTransformer


class StreamingGraphTransformer(GraphTransformer):
    """Memory-efficient transformer that processes data in streaming fashion"""
    
    def __init__(self, chunk_size: int = 100, performance_manager: Optional[UnifiedPerformanceManager] = None):
        super().__init__()  # Initialize parent GraphTransformer
        self.chunk_size = chunk_size
        self.performance_manager = performance_manager
    
    def transform_table(self, table_name: str, rows: List[Dict]) -> List[ImportOperation]:
        """Transform rows using streaming approach to minimize memory usage"""
        # Use generator-based approach to reduce memory allocation
        if self.performance_manager:
            with self.performance_manager.batch_timer():
                return list(self._stream_transform_table(table_name, rows))
        else:
            return list(self._stream_transform_table(table_name, rows))
    
    def _stream_transform_table(self, table_name: str, rows: List[Dict]) -> Iterator[ImportOperation]:
        """Generator-based transformation to minimize memory usage"""
        transformer = self.table_transformers.get(table_name)
        if transformer:
            # Process in small chunks using generator pattern (more memory efficient)
            for chunk_start in range(0, len(rows), self.chunk_size):
                chunk_end = min(chunk_start + self.chunk_size, len(rows))
                # Use slice directly instead of creating intermediate list
                chunk_operations = transformer.transform_rows(rows[chunk_start:chunk_end])
                
                # Yield operations immediately to avoid accumulating large lists
                for operation in chunk_operations:
                    yield operation
                
        else:
            # Default streaming node creation using generator
            yield from self._default_node_creation(table_name, rows)
    
    def _default_node_creation(self, table_name: str, rows: List[Dict]) -> Iterator[ImportOperation]:
        """Override parent method with streaming version for memory efficiency"""
        # Use generator pattern - no intermediate list creation
        for i in range(0, len(rows), self.chunk_size):
            chunk_end = min(i + self.chunk_size, len(rows))
            # Process chunk directly without creating intermediate list  
            for row in rows[i:chunk_end]:
                yield ImportOperation(
                    operation_type="create_node",
                    entity_type=table_name.title(),
                    properties=row
                )
