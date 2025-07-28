from typing import List, Set
from neo4j import Driver
from neo4j.exceptions import Neo4jError
import logging

from ..settings import Settings


class Neo4jSchemaManager:
    """Manages Neo4j schema creation and migration"""
    
    def __init__(self, driver: Driver):
        self.driver = driver
        self.logger = logging.getLogger(__name__)
    
    def create_schema(self, tables: List[str], settings: Settings):
        """Apply Cypher schema migrations dynamically based on requested tables"""
        try:
            table_set = set(tables)
            self.logger.info(f"Creating Neo4j schema for tables: {tables}")
            
            with self.driver.session() as session:
                # Core constraints (always needed for import tracking)
                self._create_import_tracking_constraints(session)
                
                # User-related constraints and indexes
                if self._needs_user_schema(table_set):
                    self._create_user_constraints(session)
                
                # Address-related constraints and indexes  
                if self._needs_address_schema(table_set):
                    self._create_address_constraints(session)
                
                # Relationship indexes
                if self._needs_relationship_indexes(table_set):
                    self._create_relationship_indexes(session)
                
            self.logger.info("Neo4j schema creation completed successfully")
            
        except Neo4jError as e:
            self.logger.error(f"Failed to create Neo4j schema: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error creating schema: {e}")
            raise
    
    def _needs_user_schema(self, table_set: Set[str]) -> bool:
        """Check if User node schema is needed"""
        user_tables = {'fids', 'profiles', 'nindexer_fids', 'nindexer_profiles'}
        return bool(table_set.intersection(user_tables))
    
    def _needs_address_schema(self, table_set: Set[str]) -> bool:
        """Check if Address node schema is needed"""
        address_tables = {'verifications', 'account_verifications', 'nindexer_verifications'}
        return bool(table_set.intersection(address_tables))
    
    def _needs_relationship_indexes(self, table_set: Set[str]) -> bool:
        """Check if relationship indexes are needed"""
        relationship_tables = {'follows', 'nindexer_follows', 'verifications', 'account_verifications', 'nindexer_verifications'}
        return bool(table_set.intersection(relationship_tables))
    
    def _create_import_tracking_constraints(self, session):
        """Create constraints for import tracking"""
        constraints = [
            "CREATE CONSTRAINT import_tracking_unique IF NOT EXISTS FOR (t:ImportTracking) REQUIRE (t.table_name, t.file_name) IS UNIQUE"
        ]
        
        for constraint in constraints:
            try:
                session.run(constraint)
                self.logger.debug(f"Created constraint: {constraint}")
            except Neo4jError as e:
                if "equivalent constraint already exists" in str(e).lower():
                    self.logger.debug(f"Constraint already exists: {constraint}")
                else:
                    raise
    
    def _create_user_constraints(self, session):
        """Create User node constraints and indexes"""
        constraints = [
            # Primary key constraint on FID
            "CREATE CONSTRAINT user_fid_unique IF NOT EXISTS FOR (u:User) REQUIRE u.fid IS UNIQUE",
            
            # Indexes for common query patterns
            "CREATE INDEX user_username_index IF NOT EXISTS FOR (u:User) ON (u.username)",
            "CREATE INDEX user_updated_at_index IF NOT EXISTS FOR (u:User) ON (u.updated_at)"
        ]
        
        for constraint in constraints:
            try:
                session.run(constraint)
                self.logger.debug(f"Created User constraint/index: {constraint}")
            except Neo4jError as e:
                if "equivalent constraint already exists" in str(e).lower() or "equivalent index already exists" in str(e).lower():
                    self.logger.debug(f"User constraint/index already exists: {constraint}")
                else:
                    raise
    
    def _create_address_constraints(self, session):
        """Create Address node constraints and indexes"""
        constraints = [
            # Primary key constraint on address
            "CREATE CONSTRAINT address_unique IF NOT EXISTS FOR (a:Address) REQUIRE a.address IS UNIQUE",
            
            # Indexes for queries
            "CREATE INDEX address_chain_index IF NOT EXISTS FOR (a:Address) ON (a.chain)",
            "CREATE INDEX address_updated_at_index IF NOT EXISTS FOR (a:Address) ON (a.updated_at)"
        ]
        
        for constraint in constraints:
            try:
                session.run(constraint)
                self.logger.debug(f"Created Address constraint/index: {constraint}")
            except Neo4jError as e:
                if "equivalent constraint already exists" in str(e).lower() or "equivalent index already exists" in str(e).lower():
                    self.logger.debug(f"Address constraint/index already exists: {constraint}")
                else:
                    raise
    
    def _create_relationship_indexes(self, session):
        """Create relationship indexes for performance"""
        indexes = [
            # FOLLOWS relationship indexes
            "CREATE INDEX follows_timestamp_index IF NOT EXISTS FOR ()-[r:FOLLOWS]-() ON (r.timestamp)",
            "CREATE INDEX follows_updated_at_index IF NOT EXISTS FOR ()-[r:FOLLOWS]-() ON (r.updated_at)",
            
            # VERIFIED_ADDRESS relationship indexes
            "CREATE INDEX verified_address_timestamp_index IF NOT EXISTS FOR ()-[r:VERIFIED_ADDRESS]-() ON (r.verification_timestamp)",
            "CREATE INDEX verified_address_updated_at_index IF NOT EXISTS FOR ()-[r:VERIFIED_ADDRESS]-() ON (r.updated_at)"
        ]
        
        for index in indexes:
            try:
                session.run(index)
                self.logger.debug(f"Created relationship index: {index}")
            except Neo4jError as e:
                if "equivalent index already exists" in str(e).lower():
                    self.logger.debug(f"Relationship index already exists: {index}")
                else:
                    raise
