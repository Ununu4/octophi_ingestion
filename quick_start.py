"""
Quick Start Helper Script

Interactive script to help you run your first ingestion.
"""

import sys
from pathlib import Path

print("=" * 70)
print("🐙 OCTOPHI INGESTION SYSTEM - QUICK START")
print("=" * 70)
print()

# Check if pandas is installed
try:
    import pandas
    print("✓ pandas is installed")
except ImportError:
    print("❌ pandas is not installed")
    print()
    print("Please install dependencies first:")
    print("  pip install pandas openpyxl")
    print()
    print("Or use:")
    print("  pip install -r requirements.txt")
    print()
    sys.exit(1)

print()
print("Great! The system is ready to use.")
print()
print("=" * 70)
print("EXAMPLE COMMANDS")
print("=" * 70)
print()

print("1. Test with dry run (no data inserted):")
print()
print("   python -m ingestion.cli \\")
print("       --schema monet \\")
print("       --file ../staging_leads_template.xlsx \\")
print("       --db test.sqlite \\")
print("       --source \"Test Source\" \\")
print("       --dry-run")
print()

print("2. Real ingestion to existing database:")
print()
print("   python -m ingestion.cli \\")
print("       --schema monet \\")
print("       --file ../staging_leads_template.xlsx \\")
print("       --db ../unified_database_migrated.sqlite \\")
print("       --source \"Your Source Name\"")
print()

print("3. With all options:")
print()
print("   python -m ingestion.cli \\")
print("       --schema monet \\")
print("       --file your_data.xlsx \\")
print("       --db ../unified_database_migrated.sqlite \\")
print("       --source \"Source Name\" \\")
print("       --upload-tag \"2025_12_08_batch_1\" \\")
print("       --create-indexes \\")
print("       --log-file ingestion.log")
print()

print("=" * 70)
print("NEXT STEPS")
print("=" * 70)
print()
print("1. View full help:")
print("   python -m ingestion.cli --help")
print()
print("2. Read usage guide:")
print("   USAGE_GUIDE.md")
print()
print("3. Run verification:")
print("   python verify_setup.py")
print()
print("=" * 70)




