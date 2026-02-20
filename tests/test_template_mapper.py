"""
Tests for template_mapper module (template-only mode).
"""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.template_mapper import TemplateMapper


def test_template_maps_headers():
    """Template maps headers correctly via explicit CSV."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("incoming_schema,expected_schema\n")
        f.write("Company Name,business_legal_name\n")
        f.write("Phone,phone_raw\n")
        f.write("Email,business_email\n")
        f.flush()
        path = f.name
    try:
        mapper = TemplateMapper(path)
        mapping = mapper.map_headers(['Company Name', 'Phone', 'Email', 'Unknown'])
        assert mapping['Company Name'] == 'business_legal_name'
        assert mapping['Phone'] == 'phone_raw'
        assert mapping['Email'] == 'business_email'
        assert mapping['Unknown'] is None
    finally:
        Path(path).unlink(missing_ok=True)


def test_template_combination_parsing():
    """Combination syntax 'first name + last name' -> owner_name is parsed."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("incoming_schema,expected_schema\n")
        f.write("first name + last name,owner_name\n")
        f.write("Business Name,business_legal_name\n")
        f.flush()
        path = f.name
    try:
        mapper = TemplateMapper(path)
        combos = mapper.get_combinations()
        assert len(combos) == 1
        assert combos[0]['target_field'] == 'owner_name'
        assert combos[0]['sources'] == ['first name', 'last name']
        assert combos[0]['separator'] == ' '
        mapping = mapper.map_headers(['first name', 'last name', 'Business Name'])
        assert mapping.get('first name') == '__USED_IN_COMBINATION__'
        assert mapping.get('last name') == '__USED_IN_COMBINATION__'
        assert mapping.get('Business Name') == 'business_legal_name'
    finally:
        Path(path).unlink(missing_ok=True)


def test_template_no_computations():
    """Template mapper returns no computations."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("incoming_schema,expected_schema\n")
        f.write("X,business_legal_name\n")
        f.flush()
        path = f.name
    try:
        mapper = TemplateMapper(path)
        assert mapper.get_computations() == []
    finally:
        Path(path).unlink(missing_ok=True)


if __name__ == '__main__':
    print("Running template_mapper tests...")
    test_template_maps_headers()
    print("✓ test_template_maps_headers")
    test_template_combination_parsing()
    print("✓ test_template_combination_parsing")
    test_template_no_computations()
    print("✓ test_template_no_computations")
    print("\n✅ All tests passed!")
