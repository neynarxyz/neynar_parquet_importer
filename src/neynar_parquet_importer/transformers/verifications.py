from typing import Dict, List
from ..database.base import ImportOperation
from .base import ParquetTableTransformer


class VerificationsGraphTransformer(ParquetTableTransformer):
    """Transform verification tables into User-Address HOLDS relationships"""
    
    def supports_table(self, table_name: str) -> bool:
        return table_name in ["verifications", "account_verifications", "nindexer_verifications"]
    
    def transform_rows(self, rows: List[Dict]) -> List[ImportOperation]:
        operations = []
        for row in rows:
            # Create Address node first (handles bytea properly)
            address_value = row.get("address")
            # Convert bytea to hex string if needed
            if isinstance(address_value, (bytes, bytearray)):
                address_hex = "0x" + address_value.hex()
            elif isinstance(address_value, str) and not address_value.startswith("0x"):
                address_hex = f"0x{address_value}"
            else:
                address_hex = address_value
                
            operations.append(ImportOperation(
                operation_type="create_node",
                entity_type="Address",
                properties={
                    "address": address_hex,
                    "protocol": row.get("protocol"),
                    "updated_at": row.get("updated_at"),
                },
                metadata={
                    "primary_key": "address",
                }
            ))
            
            # Create HOLDS relationship: fid holds address
            operations.append(ImportOperation(
                operation_type="create_relationship",
                entity_type="HOLDS",
                properties={
                    "source_fid": row.get("fid"),
                    "target_address": address_hex,
                    "timestamp": row.get("timestamp"),
                    "created_at": row.get("created_at"),
                    "updated_at": row.get("updated_at"),
                    "deleted_at": row.get("deleted_at"),
                    "protocol": row.get("protocol"),
                },
                metadata={
                    "source_node_type": "User",
                    "target_node_type": "Address",
                    "source_key": "fid",
                    "target_key": "address",
                }
            ))
        return operations
