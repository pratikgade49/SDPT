"""
Database models and table creation utilities.
"""

from typing import Dict, List, Any, Optional
import json

class FieldDefinition:
    """Represents a field in the Field Catalogue."""
    
    def __init__(
        self,
        field_name: str,
        data_type: str,
        field_length: Optional[int] = None,
        default_value: Optional[str] = None,
        is_characteristic: bool = False,
        characteristic_type: Optional[str] = None,
        characteristic_category: Optional[str] = None
    ):
        self.field_name = field_name
        self.data_type = data_type
        self.field_length = field_length
        self.default_value = default_value
        self.is_characteristic = is_characteristic
        self.characteristic_type = characteristic_type
        self.characteristic_category = characteristic_category
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "field_name": self.field_name,
            "data_type": self.data_type,
            "field_length": self.field_length,
            "default_value": self.default_value,
            "is_characteristic": self.is_characteristic,
            "characteristic_type": self.characteristic_type,
            "characteristic_category": self.characteristic_category
        }
    
    def get_sql_type(self) -> str:
        """Get SQL data type for this field."""
        type_mapping = {
            "Char": f"VARCHAR({self.field_length or 255})",
            "Numeric": "DECIMAL(18, 2)",
            "Date": "DATE",
            "Timestamp": "TIMESTAMP",
            "Boolean": "BOOLEAN",
            "Text": "TEXT"
        }
        return type_mapping.get(self.data_type, "VARCHAR(255)")

class TableSchemaBuilder:
    """Builds SQL DDL for dynamic tables."""
    
    @staticmethod
    def build_master_data_table(
        tenant_id: str,
        fields: List[FieldDefinition]
    ) -> str:
        """
        Build CREATE TABLE statement for Master Data.
        
        Args:
            tenant_id: Tenant identifier
            fields: List of field definitions from Field Catalogue
            
        Returns:
            CREATE TABLE SQL statement
        """
