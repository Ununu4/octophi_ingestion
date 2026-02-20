"""
Tests for schema_loader module.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.schema_loader import Schema


def test_schema_loading():
    """Test basic schema loading."""
    schema_path = Path(__file__).parent.parent / 'schemas' / 'monet' / 'schema.json'
    schema = Schema(str(schema_path))
    
    assert schema.get_schema_name() == 'monet_merchant'
    assert schema.get_version() == '1.0'


def test_get_entities():
    """Test getting entity list."""
    schema_path = Path(__file__).parent.parent / 'schemas' / 'monet' / 'schema.json'
    schema = Schema(str(schema_path))
    
    entities = schema.get_entities()
    assert 'lead' in entities
    assert 'owner' in entities


def test_get_fields():
    """Test getting field list for entity."""
    schema_path = Path(__file__).parent.parent / 'schemas' / 'monet' / 'schema.json'
    schema = Schema(str(schema_path))
    
    lead_fields = schema.fields('lead')
    assert 'business_legal_name' in lead_fields
    assert 'phone_raw' in lead_fields
    assert 'phone_clean' in lead_fields


def test_field_type():
    """Test getting field type."""
    schema_path = Path(__file__).parent.parent / 'schemas' / 'monet' / 'schema.json'
    schema = Schema(str(schema_path))
    
    assert schema.field_type('lead', 'business_legal_name') == 'string'
    assert schema.field_type('lead', 'phone_raw') == 'phone'
    assert schema.field_type('lead', 'business_email') == 'email'


def test_derived_from():
    """Test checking derived fields."""
    schema_path = Path(__file__).parent.parent / 'schemas' / 'monet' / 'schema.json'
    schema = Schema(str(schema_path))
    
    assert schema.derived_from('lead', 'phone_clean') == 'phone_raw'
    assert schema.derived_from('lead', 'business_legal_name') is None


def test_is_required():
    """Test checking required fields."""
    schema_path = Path(__file__).parent.parent / 'schemas' / 'monet' / 'schema.json'
    schema = Schema(str(schema_path))
    
    assert schema.is_required('lead', 'business_legal_name') is True
    assert schema.is_required('lead', 'dba') is False


def test_appendix_config():
    """Test appendix configuration."""
    schema_path = Path(__file__).parent.parent / 'schemas' / 'monet' / 'schema.json'
    schema = Schema(str(schema_path))
    
    assert schema.appendix_enabled() == True
    assert schema.appendix_table_name() == 'lead_appendix_kv'


if __name__ == '__main__':
    # Run tests manually
    print("Running schema_loader tests...")
    test_schema_loading()
    print("✓ test_schema_loading")
    test_get_entities()
    print("✓ test_get_entities")
    test_get_fields()
    print("✓ test_get_fields")
    test_field_type()
    print("✓ test_field_type")
    test_derived_from()
    print("✓ test_derived_from")
    test_is_required()
    print("✓ test_is_required")
    test_appendix_config()
    print("✓ test_appendix_config")
    print("\n✅ All tests passed!")




