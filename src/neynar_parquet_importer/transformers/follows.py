from typing import Dict, List
from ..database.base import ImportOperation
from .base import ParquetTableTransformer


class FollowsGraphTransformer(ParquetTableTransformer):
    """Transform follows table into User->User FOLLOWS relationships"""
    
    def supports_table(self, table_name: str) -> bool:
        return table_name in ["follows", "nindexer_follows"]
    
    def transform_rows(self, rows: List[Dict]) -> List[ImportOperation]:
        operations = []
        for row in rows:
            # Create FOLLOWS relationship: fid follows target_fid
            operations.append(ImportOperation(
                operation_type="create_relationship",
                entity_type="FOLLOWS",
                properties={
                    "source_fid": row.get("fid"),
                    "target_fid": row.get("target_fid"),
                    "timestamp": row.get("timestamp"),
                    "created_at": row.get("created_at"),
                    "updated_at": row.get("updated_at"),
                    "deleted_at": row.get("deleted_at"),
                },
                metadata={
                    "source_node_type": "User",
                    "target_node_type": "User",
                    "source_key": "fid",
                    "target_key": "fid",
                }
            ))
        return operations
