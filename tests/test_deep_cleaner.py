"""
Tests for deep_cleaner module (template-only).
"""

import sys
import tempfile
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.schema_loader import Schema
from ingestion.template_mapper import TemplateMapper
from ingestion.type_normalizer import Normalizer
from ingestion.deep_cleaner import DeepCleaner


def test_unknown_columns_go_to_appendix():
    """Unmapped columns end up in appendix_df."""
    schema_path = Path(__file__).parent.parent / 'schemas' / 'monet' / 'schema.json'
    schema = Schema(str(schema_path))
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("incoming_schema,expected_schema\n")
        f.write("Biz Name,business_legal_name\n")
        f.flush()
        template_path = f.name
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as data:
        data.write("Biz Name,Extra Col\n")
        data.write("Acme Inc,extra value\n")
        data.flush()
        data_path = data.name
    try:
        mapper = TemplateMapper(template_path)
        normalizer = Normalizer()
        cleaner = DeepCleaner(schema, mapper, normalizer)
        leads_df, owners_df, appendix_df = cleaner.clean_file(data_path, "test_tag")
        assert "Extra Col" in appendix_df["column_name"].values
        assert appendix_df["value"].iloc[0] == "extra value"
        assert list(leads_df.columns)  # has schema fields
    finally:
        Path(template_path).unlink(missing_ok=True)
        Path(data_path).unlink(missing_ok=True)


def test_derived_fields_present():
    """Derived fields like phone_clean are present in output."""
    schema_path = Path(__file__).parent.parent / 'schemas' / 'monet' / 'schema.json'
    schema = Schema(str(schema_path))
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("incoming_schema,expected_schema\n")
        f.write("Company,business_legal_name\n")
        f.write("Phone,phone_raw\n")
        f.flush()
        template_path = f.name
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as data:
        data.write("Company,Phone\n")
        data.write("Acme,(555) 123-4567\n")
        data.flush()
        data_path = data.name
    try:
        mapper = TemplateMapper(template_path)
        normalizer = Normalizer()
        cleaner = DeepCleaner(schema, mapper, normalizer)
        leads_df, _, _ = cleaner.clean_file(data_path, "test_tag")
        assert "phone_clean" in leads_df.columns
        assert leads_df["phone_clean"].iloc[0] == "5551234567"
    finally:
        Path(template_path).unlink(missing_ok=True)
        Path(data_path).unlink(missing_ok=True)


def test_required_validation():
    """validate_required_fields returns errors when required field missing or all empty."""
    schema_path = Path(__file__).parent.parent / 'schemas' / 'monet' / 'schema.json'
    schema = Schema(str(schema_path))
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("incoming_schema,expected_schema\n")
        f.write("Other,dba\n")
        f.flush()
        template_path = f.name
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as data:
        data.write("Other\n")
        data.write("x\n")
        data.flush()
        data_path = data.name
    try:
        mapper = TemplateMapper(template_path)
        normalizer = Normalizer()
        cleaner = DeepCleaner(schema, mapper, normalizer)
        leads_df, owners_df, _ = cleaner.clean_file(data_path, "test_tag")
        errors = cleaner.validate_required_fields(leads_df, owners_df)
        assert any("business_legal_name" in e for e in errors)
    finally:
        Path(template_path).unlink(missing_ok=True)
        Path(data_path).unlink(missing_ok=True)


if __name__ == '__main__':
    print("Running deep_cleaner tests...")
    test_unknown_columns_go_to_appendix()
    print("✓ test_unknown_columns_go_to_appendix")
    test_derived_fields_present()
    print("✓ test_derived_fields_present")
    test_required_validation()
    print("✓ test_required_validation")
    print("\n✅ All tests passed!")
