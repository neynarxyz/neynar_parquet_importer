from typing import Dict, Any, List
import json


class CypherQueryBuilder:
    """Generate efficient Cypher queries for Neo4j operations"""
    
    def build_node_merge_query(self, entity_type: str, sample_properties: Dict[str, Any]) -> str:
        """Build a MERGE query for creating/updating nodes"""
        
        # Determine primary key based on entity type
        if entity_type == "User":
            primary_key = "fid"
        elif entity_type == "Address":
            primary_key = "address"
        else:
            # For unknown types, use 'id' or first property as fallback
            primary_key = "id" if "id" in sample_properties else list(sample_properties.keys())[0]
        
        # Build property setters (exclude primary key from SET clause)
        set_properties = []
        for prop_name in sample_properties.keys():
            if prop_name != primary_key:
                set_properties.append(f"n.{prop_name} = node.{prop_name}")
        
        set_clause = ", ".join(set_properties) if set_properties else ""
        
        query = f"""
        UNWIND $nodes AS node
        MERGE (n:{entity_type} {{{primary_key}: node.{primary_key}}})
        """
        
        if set_clause:
            query += f"SET {set_clause}"
        
        return query.strip()
    
    def build_relationship_merge_query(
        self,
        relationship_type: str,
        source_node_type: str,
        target_node_type: str,
        source_key: str,
        target_key: str
    ) -> str:
        """Build a MERGE query for creating relationships between nodes"""
        
        # Extract source and target identifiers from relationship properties
        if relationship_type == "FOLLOWS":
            source_prop = "source_fid"
            target_prop = "target_fid"
        elif relationship_type == "HOLDS":
            source_prop = "source_fid"
            target_prop = "target_address"
        elif relationship_type == "VERIFIED_ADDRESS":
            source_prop = "source_fid"
            target_prop = "target_address"
        else:
            # Default naming convention
            source_prop = f"source_{source_key}"
            target_prop = f"target_{target_key}"
        
        # Build property setters for relationship
        rel_properties = []
        if relationship_type == "FOLLOWS":
            rel_properties = [
                "r.timestamp = rel.timestamp",
                "r.created_at = rel.created_at", 
                "r.updated_at = rel.updated_at",
                "r.deleted_at = rel.deleted_at"
            ]
        elif relationship_type == "HOLDS":
            rel_properties = [
                "r.timestamp = rel.timestamp",
                "r.created_at = rel.created_at",
                "r.updated_at = rel.updated_at", 
                "r.deleted_at = rel.deleted_at",
                "r.protocol = rel.protocol"
            ]
        elif relationship_type == "VERIFIED_ADDRESS":
            rel_properties = [
                "r.verification_timestamp = rel.verification_timestamp",
                "r.updated_at = rel.updated_at"
            ]
        
        set_clause = ", ".join(rel_properties) if rel_properties else ""
        
        query = f"""
        UNWIND $relationships AS rel
        MERGE (source:{source_node_type} {{{source_key}: rel.{source_prop}}})
        MERGE (target:{target_node_type} {{{target_key}: rel.{target_prop}}})
        MERGE (source)-[r:{relationship_type}]->(target)
        """
        
        if set_clause:
            query += f"SET {set_clause}"
        
        return query.strip()
    
    def build_import_progress_query(self, table_name: str, file_name: str, row_group: int) -> str:
        """Build query to update import progress tracking"""
        return """
        MERGE (t:ImportTracking {table_name: $table_name, file_name: $file_name})
        SET t.last_row_group = $row_group, t.updated_at = datetime()
        """
    
    def build_count_query(self, entity_type: str) -> str:
        """Build query to count nodes or relationships"""
        if entity_type.isupper():  # Relationship type (e.g., "FOLLOWS")
            return f"MATCH ()-[r:{entity_type}]-() RETURN count(r) as count"
        else:  # Node type (e.g., "User")
            return f"MATCH (n:{entity_type}) RETURN count(n) as count"
    
    def build_sample_query(self, entity_type: str, limit: int = 5) -> str:
        """Build query to get sample nodes or relationships for validation"""
        if entity_type.isupper():  # Relationship type
            return f"""
            MATCH (a)-[r:{entity_type}]->(b) 
            RETURN a, r, b 
            LIMIT {limit}
            """
        else:  # Node type
            return f"""
            MATCH (n:{entity_type}) 
            RETURN n 
            LIMIT {limit}
            """
