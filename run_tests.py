"""
Test runner for OCTOPHI Ingestion System.

Runs all test modules and reports results.
"""

import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

print("=" * 70)
print("🐙 OCTOPHI INGESTION SYSTEM V1 - TEST SUITE")
print("=" * 70)
print()

# Import test modules
try:
    from tests import test_schema_loader
    from tests import test_header_mapper
    from tests import test_normalization
    
    all_passed = True
    
    # Run schema loader tests
    print("1️⃣  TESTING: schema_loader.py")
    print("-" * 70)
    try:
        test_schema_loader.test_schema_loading()
        test_schema_loader.test_get_entities()
        test_schema_loader.test_get_fields()
        test_schema_loader.test_field_type()
        test_schema_loader.test_derived_from()
        test_schema_loader.test_is_required()
        test_schema_loader.test_appendix_config()
        print("✅ All schema_loader tests passed\n")
    except Exception as e:
        print(f"❌ schema_loader tests failed: {e}\n")
        all_passed = False
    
    # Run header mapper tests
    print("2️⃣  TESTING: header_mapper.py")
    print("-" * 70)
    try:
        test_header_mapper.test_header_mapping()
        test_header_mapper.test_header_normalization()
        test_header_mapper.test_multiple_headers()
        test_header_mapper.test_unmapped_headers()
        print("✅ All header_mapper tests passed\n")
    except Exception as e:
        print(f"❌ header_mapper tests failed: {e}\n")
        all_passed = False
    
    # Run normalization tests
    print("3️⃣  TESTING: type_normalizer.py")
    print("-" * 70)
    try:
        test_normalization.test_phone_normalization()
        test_normalization.test_email_normalization()
        test_normalization.test_state_normalization()
        test_normalization.test_zip_normalization()
        test_normalization.test_id_number_normalization()
        test_normalization.test_date_normalization()
        test_normalization.test_placeholder_detection()
        test_normalization.test_phone_clean_derivation()
        print("✅ All type_normalizer tests passed\n")
    except Exception as e:
        print(f"❌ type_normalizer tests failed: {e}\n")
        all_passed = False
    
    # Final result
    print("=" * 70)
    if all_passed:
        print("🎉 ALL TESTS PASSED!")
    else:
        print("⚠️  SOME TESTS FAILED")
    print("=" * 70)
    
    sys.exit(0 if all_passed else 1)
    
except ImportError as e:
    print(f"❌ Failed to import test modules: {e}")
    print("\nMake sure you're running from the octophi_ingestion directory:")
    print("  cd octophi_ingestion")
    print("  python run_tests.py")
    sys.exit(1)




