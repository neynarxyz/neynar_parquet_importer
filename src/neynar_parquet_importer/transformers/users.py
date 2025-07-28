from typing import Dict, List
from ..database.base import ImportOperation
from .base import ParquetTableTransformer


class UsersGraphTransformer(ParquetTableTransformer):
    """Transform user tables (fids, profiles) into User nodes"""
    
    def supports_table(self, table_name: str) -> bool:
        return table_name in ["fids", "profiles", "nindexer_profiles", "nindexer_fids"]
    
    def transform_rows(self, rows: List[Dict]) -> List[ImportOperation]:
        operations = []
        for row in rows:
            # Create or update User node
            user_properties = {
                "fid": row.get("fid"),
                "updated_at": row.get("updated_at"),
            }
            
            # Add additional properties based on table type
            if "username" in row:
                user_properties["username"] = row.get("username")
            if "display_name" in row:
                user_properties["display_name"] = row.get("display_name")
            if "pfp_url" in row:
                user_properties["pfp_url"] = row.get("pfp_url")
            if "bio" in row:
                user_properties["bio"] = row.get("bio")
            if "follower_count" in row:
                user_properties["follower_count"] = row.get("follower_count")
            if "following_count" in row:
                user_properties["following_count"] = row.get("following_count")
                
            operations.append(ImportOperation(
                operation_type="create_node",
                entity_type="User",
                properties=user_properties,
                metadata={
                    "primary_key": "fid",
                }
            ))
        return operations
