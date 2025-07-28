from typing import Any, List, Optional
from neo4j import GraphDatabase, Driver
from neo4j.exceptions import Neo4jError
from tenacity import retry, stop_after_attempt, wait_exponential_jitter
import logging
import gc

from .base import DatabaseBackend, ImportOperation
from .unified_performance import create_performance_manager
from ..settings import Settings


class Neo4jBackend(DatabaseBackend):
    """Neo4j implementation with native Neo4j progress tracking and performance optimization"""
    
    def __init__(self):
        self.driver: Optional[Driver] = None
        self.schema_manager: Optional['Neo4jSchemaManager'] = None
        self.query_builder: Optional['CypherQueryBuilder'] = None
        self.logger = logging.getLogger(__name__)
        
        # Performance optimization components - using unified manager
        self.unified_performance = create_performance_manager("neo4j")
    
    def init_db(self, uri: str, tables: List[str], settings: Settings) -> Any:
        """Initialize database connection and apply schema migrations"""
        try:
            # Start performance monitoring
            self.unified_performance.start_monitoring()
            
            # Create Neo4j driver
            self.driver = GraphDatabase.driver(
                settings.neo4j_uri,
                auth=(settings.neo4j_user, settings.neo4j_password),
                database=settings.neo4j_database
            )
            
            # Test connection
            self.driver.verify_connectivity()
            self.logger.info(f"Connected to Neo4j at {settings.neo4j_uri}")
            
            # Initialize schema management
            from .neo4j_schema import Neo4jSchemaManager
            from .neo4j_queries import CypherQueryBuilder
            
            self.schema_manager = Neo4jSchemaManager(self.driver)
            self.query_builder = CypherQueryBuilder()
            
            # Apply schema migrations for requested tables
            self.schema_manager.create_schema(tables, settings)
            
            return self.driver
            
        except Neo4jError as e:
            self.logger.error(f"Failed to connect to Neo4j: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error initializing Neo4j: {e}")
            raise
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=1, max=60),
        reraise=True,
    )
    def import_operations(self, operations: List[ImportOperation]) -> None:
        """Import a batch of database operations with performance optimization"""
        if not operations:
            return
        
        if not self.driver or not self.query_builder:
            raise RuntimeError("Neo4j backend not initialized")
        
        # Check memory usage before processing (non-blocking)
        memory_check = self.unified_performance.check_memory_pressure()
        if memory_check['should_pause']:
            self.logger.warning("High memory usage detected, consider reducing batch size")
            # Note: Removed gc.collect() to avoid blocking all threads during import
        
        try:
            with self.unified_performance.batch_timer():
                # Check memory pressure before processing
                memory_check = self.unified_performance.check_memory_pressure()
                if memory_check['should_pause']:
                    gc.collect()
                    self.logger.warning("Memory pressure detected, triggered garbage collection")
                
                # Group operations by type for optimized processing
                node_operations = [op for op in operations if op.operation_type == "create_node"]
                relationship_operations = [op for op in operations if op.operation_type == "create_relationship"]
                
                with self.driver.session() as session:
                    with session.begin_transaction() as tx:
                        # Process node operations first
                        if node_operations:
                            self.logger.info(f"🔵 Processing {len(node_operations)} node operations")
                            self._process_node_operations_optimized(tx, node_operations)
                        
                        # Process relationship operations
                        if relationship_operations:
                            self.logger.info(f"🔗 Processing {len(relationship_operations)} relationship operations")
                            self._process_relationship_operations_optimized(tx, relationship_operations)
                        
                        # Commit transaction
                        self.logger.info(f"💾 Committing transaction with {len(operations)} total operations")
                        tx.commit()
                        self.logger.info(f"✅ Transaction committed successfully")
                
                # Record performance metrics
                self.unified_performance.record_operations(len(operations))
                
                self.logger.debug(f"Successfully imported {len(operations)} operations")
                
        except Neo4jError as e:
            self.unified_performance.record_error()
            self.logger.error(f"Neo4j error during import: {e}")
            raise
        except Exception as e:
            self.unified_performance.record_error()
            self.logger.error(f"Unexpected error during import: {e}")
            raise
    
    def _process_node_operations_optimized(self, tx, operations: List[ImportOperation]) -> None:
        """Process node creation operations with performance optimization"""
        import time
        
        # Group by entity type for efficient batch processing
        nodes_by_type = {}
        for op in operations:
            entity_type = op.entity_type
            if entity_type not in nodes_by_type:
                nodes_by_type[entity_type] = []
            nodes_by_type[entity_type].append(op.properties)
        
        # Execute batch creation for each node type with performance tracking
        for entity_type, node_properties_list in nodes_by_type.items():
            start_time = time.time()
            
            # Use reasonable batch size
            optimal_batch_size = self.unified_performance.current_batch_size
            
            # Process in optimized batches
            for i in range(0, len(node_properties_list), optimal_batch_size):
                batch = node_properties_list[i:i + optimal_batch_size]
                query = self.query_builder.build_node_merge_query(entity_type, batch[0])
                self.logger.info(f"🔵 Executing node query for {entity_type}: {query}")
                self.logger.info(f"   With {len(batch)} nodes: {batch}")
                result = tx.run(query, nodes=batch)
                self.logger.info(f"   Query executed, summary: {result.consume().counters}")
            
            # Record query performance
            execution_time = time.time() - start_time
            self.logger.debug(f"Node creation took {execution_time:.3f}s for {len(node_properties_list)} nodes")
    
    def _process_relationship_operations_optimized(self, tx, operations: List[ImportOperation]) -> None:
        """Process relationship creation operations with performance optimization"""
        import time
        
        # Group by relationship type for efficient batch processing
        rels_by_type = {}
        for op in operations:
            rel_type = op.entity_type
            if rel_type not in rels_by_type:
                rels_by_type[rel_type] = []
            rels_by_type[rel_type].append({
                'properties': op.properties,
                'metadata': op.metadata
            })
        
        # Execute batch creation for each relationship type with performance tracking
        for rel_type, rel_data_list in rels_by_type.items():
            if rel_data_list and rel_data_list[0]['metadata']:
                start_time = time.time()
                
                # Use reasonable batch size for relationships
                optimal_batch_size = self.unified_performance.current_batch_size
                
                metadata = rel_data_list[0]['metadata']
                query = self.query_builder.build_relationship_merge_query(
                    rel_type, 
                    metadata.get('source_node_type', 'User'),
                    metadata.get('target_node_type', 'User'),
                    metadata.get('source_key', 'fid'),
                    metadata.get('target_key', 'fid')
                )
                
                # Process in optimized batches
                for i in range(0, len(rel_data_list), optimal_batch_size):
                    batch = rel_data_list[i:i + optimal_batch_size]
                    rel_properties = [item['properties'] for item in batch]
                    self.logger.info(f"🔗 Executing relationship query for {rel_type}: {query}")
                    self.logger.info(f"   With {len(rel_properties)} relationships: {rel_properties}")
                    result = tx.run(query, relationships=rel_properties)
                    self.logger.info(f"   Query executed, summary: {result.consume().counters}")
                
                # Record query performance
                execution_time = time.time() - start_time
                self.logger.debug(f"Relationship creation took {execution_time:.3f}s for {len(rel_data_list)} relationships")
    
    def check_import_progress(self, table_name: str, file_name: str) -> Optional[int]:
        """Check last imported row group for a file"""
        if not self.driver:
            return None
        
        try:
            with self.driver.session() as session:
                # Check if import tracking node exists
                query = """
                MATCH (t:ImportTracking {table_name: $table_name, file_name: $file_name})
                RETURN t.last_row_group as last_row_group
                """
                result = session.run(query, table_name=table_name, file_name=file_name)
                record = result.single()
                return record['last_row_group'] if record else None
                
        except Neo4jError as e:
            self.logger.error(f"Error checking import progress: {e}")
            return None
    
    def mark_completed(self, file_names: List[str], table_name: str) -> None:
        """Mark files as completed in tracking system"""
        if not self.driver or not file_names:
            return
        
        try:
            with self.driver.session() as session:
                query = """
                UNWIND $file_names as file_name
                MERGE (t:ImportTracking {table_name: $table_name, file_name: file_name})
                SET t.completed = true, t.completed_at = datetime()
                """
                session.run(query, file_names=file_names, table_name=table_name)
                
        except Neo4jError as e:
            self.logger.error(f"Error marking files as completed: {e}")
            raise
    
    def close(self) -> None:
        """Clean up database connections and log performance summary"""
        try:
            # Stop performance monitoring and log summary
            # Note: unified_performance provides more detailed summary than old performance_monitor
            metrics = self.unified_performance.get_current_metrics()
            self.logger.info(f"Neo4j Performance Summary: {metrics.operations_count} ops, "
                           f"{metrics.operations_per_second:.2f} ops/sec, "
                           f"peak memory: {metrics.peak_memory_mb:.2f}MB")
            
            # Log performance summary
            metrics = self.unified_performance.get_current_metrics()
            self.logger.info(f"Neo4j Performance Summary: {metrics.operations_count} ops, "
                           f"{metrics.operations_per_second:.2f} ops/sec, "
                           f"peak memory: {metrics.peak_memory_mb:.2f}MB")
            
            # Stop unified performance monitoring
            self.unified_performance.stop_monitoring()
            
            if self.driver:
                self.driver.close()
                self.driver = None
                self.logger.info("Neo4j connection closed")
                
        except Exception as e:
            self.logger.error(f"Error during Neo4j cleanup: {e}")
    
    def get_performance_metrics(self):
        """Get current performance metrics for monitoring"""
        memory_check = self.unified_performance.check_memory_pressure()
        return {
            'performance': self.unified_performance.get_current_metrics(),
            'memory': memory_check['usage']
        }
