"""
Tests for type_normalizer module.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.type_normalizer import Normalizer


def test_phone_normalization():
    """Test phone number normalization."""
    normalizer = Normalizer()
    
    assert normalizer.normalize('(555) 123-4567', 'phone') == '5551234567'
    assert normalizer.normalize('555-123-4567', 'phone') == '5551234567'
    assert normalizer.normalize('555.123.4567', 'phone') == '5551234567'
    assert normalizer.normalize('5551234567', 'phone') == '5551234567'


def test_email_normalization():
    """Test email normalization."""
    normalizer = Normalizer()
    
    assert normalizer.normalize('USER@EXAMPLE.COM', 'email') == 'user@example.com'
    assert normalizer.normalize('  user@example.com  ', 'email') == 'user@example.com'


def test_state_normalization():
    """Test state code normalization."""
    normalizer = Normalizer()
    
    assert normalizer.normalize('ca', 'state') == 'CA'
    assert normalizer.normalize('CA', 'state') == 'CA'
    assert normalizer.normalize(' ca ', 'state') == 'CA'


def test_zip_normalization():
    """Test ZIP code normalization."""
    normalizer = Normalizer()
    
    assert normalizer.normalize('12345', 'zip') == '12345'
    assert normalizer.normalize('12345-6789', 'zip') == '12345'
    assert normalizer.normalize('12345 6789', 'zip') == '12345'


def test_id_number_normalization():
    """Test ID number normalization."""
    normalizer = Normalizer()
    
    assert normalizer.normalize('123-45-6789', 'id_number') == '123456789'
    assert normalizer.normalize('12-3456789', 'id_number') == '123456789'
    assert normalizer.normalize('123456789', 'id_number') == '123456789'


def test_date_normalization():
    """Test date normalization."""
    normalizer = Normalizer()
    
    assert normalizer.normalize('2020-01-15', 'date') == '2020-01-15'
    assert normalizer.normalize('01/15/2020', 'date') == '2020-01-15'
    assert normalizer.normalize('01-15-2020', 'date') == '2020-01-15'


def test_placeholder_detection():
    """Test placeholder value detection."""
    normalizer = Normalizer()
    
    assert normalizer.normalize('NA', 'string') is None
    assert normalizer.normalize('N/A', 'string') is None
    assert normalizer.normalize('NULL', 'string') is None
    assert normalizer.normalize('UNKNOWN', 'string') is None
    assert normalizer.normalize('', 'string') is None


def test_phone_clean_derivation():
    """Test phone_clean derivation from phone_raw."""
    normalizer = Normalizer()
    
    assert normalizer.derive_phone_clean('(555) 123-4567') == '5551234567'
    assert normalizer.derive_phone_clean('') is None
    assert normalizer.derive_phone_clean(None) is None


if __name__ == '__main__':
    # Run tests manually
    print("Running type_normalizer tests...")
    test_phone_normalization()
    print("✓ test_phone_normalization")
    test_email_normalization()
    print("✓ test_email_normalization")
    test_state_normalization()
    print("✓ test_state_normalization")
    test_zip_normalization()
    print("✓ test_zip_normalization")
    test_id_number_normalization()
    print("✓ test_id_number_normalization")
    test_date_normalization()
    print("✓ test_date_normalization")
    test_placeholder_detection()
    print("✓ test_placeholder_detection")
    test_phone_clean_derivation()
    print("✓ test_phone_clean_derivation")
    print("\n✅ All tests passed!")




