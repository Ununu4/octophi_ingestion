"""
Tests for header_mapper module.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.header_mapper import HeaderMapper


def test_header_mapping():
    """Test basic header mapping."""
    fuzzy_path = Path(__file__).parent.parent / 'schemas' / 'monet' / 'fuzzy.json'
    mapper = HeaderMapper(str(fuzzy_path))
    
    # Test exact matches (after normalization)
    assert mapper.get_canonical_field('business name') == 'business_legal_name'
    assert mapper.get_canonical_field('BUSINESS NAME') == 'business_legal_name'
    assert mapper.get_canonical_field('Business Name') == 'business_legal_name'


def test_header_normalization():
    """Test header normalization."""
    fuzzy_path = Path(__file__).parent.parent / 'schemas' / 'monet' / 'fuzzy.json'
    mapper = HeaderMapper(str(fuzzy_path))
    
    # Test with symbols and spaces
    assert mapper.get_canonical_field('business-name') == 'business_legal_name'
    assert mapper.get_canonical_field('business_name') == 'business_legal_name'
    assert mapper.get_canonical_field('  business name  ') == 'business_legal_name'


def test_multiple_headers():
    """Test mapping multiple headers."""
    fuzzy_path = Path(__file__).parent.parent / 'schemas' / 'monet' / 'fuzzy.json'
    mapper = HeaderMapper(str(fuzzy_path))
    
    headers = ['Business Name', 'Phone', 'Email', 'State', 'Unknown Column']
    mapping = mapper.map_headers(headers)
    
    assert mapping['Business Name'] == 'business_legal_name'
    assert mapping['Phone'] == 'phone_raw'
    assert mapping['Email'] == 'business_email'
    assert mapping['State'] == 'business_state'
    assert mapping['Unknown Column'] is None


def test_unmapped_headers():
    """Test getting unmapped headers."""
    fuzzy_path = Path(__file__).parent.parent / 'schemas' / 'monet' / 'fuzzy.json'
    mapper = HeaderMapper(str(fuzzy_path))
    
    headers = ['Business Name', 'Phone', 'Extra Column 1', 'Extra Column 2']
    unmapped = mapper.get_unmapped_headers(headers)
    
    assert 'Extra Column 1' in unmapped
    assert 'Extra Column 2' in unmapped
    assert 'Business Name' not in unmapped


if __name__ == '__main__':
    # Run tests manually
    print("Running header_mapper tests...")
    test_header_mapping()
    print("✓ test_header_mapping")
    test_header_normalization()
    print("✓ test_header_normalization")
    test_multiple_headers()
    print("✓ test_multiple_headers")
    test_unmapped_headers()
    print("✓ test_unmapped_headers")
    print("\n✅ All tests passed!")




