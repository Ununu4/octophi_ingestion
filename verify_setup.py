"""
Quick verification script for OCTOPHI Ingestion System.

Tests that all modules can be imported and basic functionality works.
"""

import sys
from pathlib import Path

print("=" * 70)
print("🐙 OCTOPHI INGESTION SYSTEM V1 - SETUP VERIFICATION")
print("=" * 70)
print()

try:
    # Test imports
    print("1. Testing module imports...")
    from ingestion.schema_loader import Schema
    from ingestion.header_mapper import HeaderMapper
    from ingestion.type_normalizer import Normalizer
    from ingestion.deep_cleaner import DeepCleaner
    from ingestion.ingest_engine import IngestEngine
    from utils.logger import setup_logger
    from utils.file_ops import validate_file_path
    print("   ✓ All modules imported successfully")
    print()
    
    # Test schema loading
    print("2. Testing schema loading...")
    schema_path = Path(__file__).parent / 'schemas' / 'monet' / 'schema.json'
    schema = Schema(str(schema_path))
    print(f"   ✓ Schema: {schema.get_schema_name()} v{schema.get_version()}")
    print(f"   ✓ Entities: {', '.join(schema.get_entities())}")
    print(f"   ✓ Lead fields: {len(schema.fields('lead'))}")
    print(f"   ✓ Owner fields: {len(schema.fields('owner'))}")
    print()
    
    # Test fuzzy mapping
    print("3. Testing fuzzy header mapping...")
    fuzzy_path = Path(__file__).parent / 'schemas' / 'monet' / 'fuzzy.json'
    mapper = HeaderMapper(str(fuzzy_path))
    test_headers = ['Business Name', 'Phone', 'Email', 'Unknown Column']
    mapping = mapper.map_headers(test_headers)
    print(f"   ✓ Mapped 'Business Name' → {mapping['Business Name']}")
    print(f"   ✓ Mapped 'Phone' → {mapping['Phone']}")
    print(f"   ✓ Unmapped 'Unknown Column' → {mapping['Unknown Column']}")
    print()
    
    # Test normalization
    print("4. Testing type normalization...")
    normalizer = Normalizer()
    phone = normalizer.normalize('(555) 123-4567', 'phone')
    email = normalizer.normalize('USER@EXAMPLE.COM', 'email')
    state = normalizer.normalize('ca', 'state')
    print(f"   ✓ Phone: '(555) 123-4567' → '{phone}'")
    print(f"   ✓ Email: 'USER@EXAMPLE.COM' → '{email}'")
    print(f"   ✓ State: 'ca' → '{state}'")
    print()
    
    # Success!
    print("=" * 70)
    print("✅ ALL VERIFICATION CHECKS PASSED!")
    print("=" * 70)
    print()
    print("System is ready to use. Try running:")
    print()
    print("  python -m ingestion.cli --help")
    print()
    
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)
except FileNotFoundError as e:
    print(f"❌ File not found: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)




