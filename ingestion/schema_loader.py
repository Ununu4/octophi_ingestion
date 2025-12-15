"""
Schema Loader Module

Loads and parses schema.json to provide field metadata for ingestion.
"""

import json
from pathlib import Path
from typing import Optional, List, Dict, Any


class Schema:
    """Loads and provides access to schema metadata."""
    
    def __init__(self, path: str):
        """
        Initialize schema loader.
        
        Args:
            path: Path to schema.json file
        """
        self.path = Path(path)
        self.schema_data = self._load_schema()
        
    def _load_schema(self) -> dict:
        """Load schema from JSON file."""
        if not self.path.exists():
            raise FileNotFoundError(f"Schema file not found: {self.path}")
        
        with open(self.path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def fields(self, entity: str) -> List[str]:
        """
        Get list of all fields for an entity.
        
        Args:
            entity: Entity name (e.g., 'lead', 'owner')
            
        Returns:
            List of field names
        """
        if entity not in self.schema_data.get('entities', {}):
            raise ValueError(f"Entity '{entity}' not found in schema")
        
        return list(self.schema_data['entities'][entity]['fields'].keys())
    
    def field_type(self, entity: str, field: str) -> str:
        """
        Get the type of a specific field.
        
        Args:
            entity: Entity name
            field: Field name
            
        Returns:
            Field type string (e.g., 'string', 'phone', 'email')
        """
        if entity not in self.schema_data.get('entities', {}):
            raise ValueError(f"Entity '{entity}' not found in schema")
        
        fields = self.schema_data['entities'][entity]['fields']
        if field not in fields:
            raise ValueError(f"Field '{field}' not found in entity '{entity}'")
        
        return fields[field].get('type', 'string')
    
    def derived_from(self, entity: str, field: str) -> Optional[str]:
        """
        Get the source field if this is a derived field.
        
        Args:
            entity: Entity name
            field: Field name
            
        Returns:
            Source field name if derived, None otherwise
        """
        if entity not in self.schema_data.get('entities', {}):
            return None
        
        fields = self.schema_data['entities'][entity]['fields']
        if field not in fields:
            return None
        
        return fields[field].get('derived_from')
    
    def is_required(self, entity: str, field: str) -> bool:
        """
        Check if a field is required.
        
        Args:
            entity: Entity name
            field: Field name
            
        Returns:
            True if field is required, False otherwise
        """
        if entity not in self.schema_data.get('entities', {}):
            return False
        
        fields = self.schema_data['entities'][entity]['fields']
        if field not in fields:
            return False
        
        return fields[field].get('required', False)
    
    def is_system_generated(self, entity: str, field: str) -> bool:
        """
        Check if a field is system-generated.
        
        Args:
            entity: Entity name
            field: Field name
            
        Returns:
            True if field is system-generated, False otherwise
        """
        if entity not in self.schema_data.get('entities', {}):
            return False
        
        fields = self.schema_data['entities'][entity]['fields']
        if field not in fields:
            return False
        
        return fields[field].get('system_generated', False)
    
    def get_entities(self) -> List[str]:
        """
        Get list of all entities in schema.
        
        Returns:
            List of entity names
        """
        return list(self.schema_data.get('entities', {}).keys())
    
    def appendix_enabled(self) -> bool:
        """Check if appendix is enabled."""
        return self.schema_data.get('appendix', {}).get('enabled', False)
    
    def appendix_table_name(self) -> str:
        """Get appendix table name."""
        return self.schema_data.get('appendix', {}).get('table_name', 'lead_appendix_kv')
    
    def get_schema_name(self) -> str:
        """Get schema name."""
        return self.schema_data.get('schema_name', 'unknown')
    
    def get_version(self) -> str:
        """Get schema version."""
        return self.schema_data.get('version', '1.0')




