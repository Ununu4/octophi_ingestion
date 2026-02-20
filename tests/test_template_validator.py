"""
Tests for template_validator (strict template validation before cleaning).
"""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.schema_loader import Schema
from ingestion.template_mapper import TemplateMapper
from ingestion.template_validator import validate_template


def test_valid_template_passes():
    """Template that maps required field and only schema fields passes."""
    schema_path = Path(__file__).parent.parent / 'schemas' / 'monet' / 'schema.json'
    schema = Schema(str(schema_path))
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("incoming_schema,expected_schema\n")
        f.write("Company,business_legal_name\n")
        f.write("Phone,phone_raw\n")
        f.flush()
        path = f.name
    try:
        mapper = TemplateMapper(path)
        errors = validate_template(schema, mapper)
        assert errors == [], errors
    finally:
        Path(path).unlink(missing_ok=True)


def test_unknown_expected_field_fails():
    """Template that maps to a field not in schema fails."""
    schema_path = Path(__file__).parent.parent / 'schemas' / 'monet' / 'schema.json'
    schema = Schema(str(schema_path))
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("incoming_schema,expected_schema\n")
        f.write("X,nonexistent_field\n")
        f.flush()
        path = f.name
    try:
        mapper = TemplateMapper(path)
        errors = validate_template(schema, mapper)
        assert any("not in schema" in e for e in errors), errors
    finally:
        Path(path).unlink(missing_ok=True)


def test_required_field_not_mapped_fails():
    """Template that does not map required business_legal_name fails."""
    schema_path = Path(__file__).parent.parent / 'schemas' / 'monet' / 'schema.json'
    schema = Schema(str(schema_path))
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("incoming_schema,expected_schema\n")
        f.write("Other,dba\n")
        f.flush()
        path = f.name
    try:
        mapper = TemplateMapper(path)
        errors = validate_template(schema, mapper)
        assert any("business_legal_name" in e for e in errors), errors
    finally:
        Path(path).unlink(missing_ok=True)


if __name__ == '__main__':
    print("Running template_validator tests...")
    test_valid_template_passes()
    print("OK test_valid_template_passes")
    test_unknown_expected_field_fails()
    print("OK test_unknown_expected_field_fails")
    test_required_field_not_mapped_fails()
    print("OK test_required_field_not_mapped_fails")
    print("All template_validator tests passed.")
