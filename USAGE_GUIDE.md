# 🐙 OCTOPHI Ingestion System - Usage Guide

Complete guide for using the OCTOPHI Ingestion System V1.

---

## Prerequisites

Before using the system, ensure you have:

1. **Python 3.9+** installed
2. **pandas** library (install if needed):
   ```bash
   pip install pandas openpyxl
   ```
   Or use the requirements file:
   ```bash
   cd octophi_ingestion
   pip install -r requirements.txt
   ```

---

## Quick Start

### Step 1: Verify Installation

```bash
cd octophi_ingestion
python verify_setup.py
```

Expected output:
```
======================================================================
🐙 OCTOPHI INGESTION SYSTEM V1 - SETUP VERIFICATION
======================================================================

1. Testing module imports...
   ✓ All modules imported successfully

2. Testing schema loading...
   ✓ Schema: monet_merchant v1.0
   ✓ Entities: lead, owner
   ✓ Lead fields: 17
   ✓ Owner fields: 13

3. Testing fuzzy header mapping...
   ✓ Mapped 'Business Name' → business_legal_name
   ✓ Mapped 'Phone' → phone_raw
   ✓ Unmapped 'Unknown Column' → None

4. Testing type normalization...
   ✓ Phone: '(555) 123-4567' → '5551234567'
   ✓ Email: 'USER@EXAMPLE.COM' → 'user@example.com'
   ✓ State: 'ca' → 'CA'

======================================================================
✅ ALL VERIFICATION CHECKS PASSED!
======================================================================
```

### Step 2: View CLI Help

```bash
python -m ingestion.cli --help
```

### Step 3: Run Your First Ingestion

```bash
python -m ingestion.cli \
    --schema monet \
    --file /path/to/your/leads.xlsx \
    --db /path/to/database.sqlite \
    --source "My First Source"
```

---

## Usage Examples

### Example 1: Basic CSV Ingestion

```bash
python -m ingestion.cli \
    --schema monet \
    --file ../staging_leads_template.xlsx \
    --db ../unified_database_migrated.sqlite \
    --source "test_source"
```

**What happens:**
1. Loads the Excel file
2. Maps headers to canonical fields using fuzzy matching
3. Normalizes all values (phones, emails, dates, etc.)
4. Inserts leads into `leads` table
5. Inserts owners into `owners` table (one per lead)
6. Stores extra columns in `lead_appendix_kv` table

### Example 2: Dry Run (Preview Only)

Test without inserting data:

```bash
python -m ingestion.cli \
    --schema monet \
    --file my_leads.csv \
    --db monet.sqlite \
    --source "Test Source" \
    --dry-run
```

Output shows:
- Number of rows that would be inserted
- Preview of first lead record
- No actual database changes

### Example 3: With Custom Upload Tag

```bash
python -m ingestion.cli \
    --schema monet \
    --file broker_feed.xlsx \
    --db monet.sqlite \
    --source "XYZ Broker" \
    --upload-tag "2025_12_08_morning_batch"
```

Upload tag is used to track which rows came from which upload batch.

### Example 4: With Index Creation

For better query performance on large databases:

```bash
python -m ingestion.cli \
    --schema monet \
    --file large_dataset.csv \
    --db monet.sqlite \
    --source "Big Data Feed" \
    --create-indexes
```

Creates indexes on:
- `leads.business_state`
- `leads.zip`
- `leads.source_id`
- `leads.phone_clean`
- `leads.business_email`
- `owners.lead_id`
- `owners.owner_phone_clean`
- `lead_appendix_kv.lead_id`
- `lead_appendix_kv.column_name`
- `lead_appendix_kv.upload_tag`

### Example 5: With Logging to File

```bash
python -m ingestion.cli \
    --schema monet \
    --file data.xlsx \
    --db monet.sqlite \
    --source "Source Name" \
    --log-file ingestion_20251208.log
```

All output is written to both console and log file.

---

## Understanding the Output

### Successful Ingestion

```
======================================================================
🐙 OCTOPHI INGESTION SYSTEM V1
======================================================================

INFO | Validating input file: leads.xlsx
INFO | ✓ File validated: 1.23 MB
INFO | Loading schema: monet
INFO | ✓ Schema loaded: monet_merchant v1.0
INFO | Loading fuzzy mappings...
INFO | ✓ Fuzzy mappings loaded
INFO | Initializing deep cleaner...

INFO | 🧹 Cleaning file: leads.xlsx
----------------------------------------------------------------------
📋 Loaded 100 rows with 30 columns
✓ Mapped 27 known fields
📦 Found 3 extra columns (will store in appendix): offer, bank, notes
✅ Cleaning complete: 100 leads, 100 owners, 300 appendix rows
----------------------------------------------------------------------

INFO | ✓ Validation passed

📊 CLEANING SUMMARY
----------------------------------------------------------------------
Leads:        100 rows
Owners:       100 rows
Appendix:     300 rows
----------------------------------------------------------------------

INFO | Connecting to database: monet.sqlite
🚀 Starting ingestion...
   Source: My Source
   Upload tag: 20251208_143052
   Records: 100 leads, 100 owners, 300 appendix rows
✓ Source ID: 5
✓ Inserted 100 leads
✓ Inserted 100 owners
✓ Inserted 300 appendix rows
✅ Ingestion complete!

======================================================================
✅ INGESTION COMPLETE
======================================================================
Source:      My Source
Upload tag:  20251208_143052
Database:    monet.sqlite
Leads:       100
Owners:      100
Appendix:    300
======================================================================
```

### Error: Missing Required Field

```
❌ Validation failed:
   • Required lead field is all empty: business_legal_name
```

**Solution:** Ensure your CSV has a column that maps to `business_legal_name` (e.g., "Business Name", "Company Name", "Legal Name")

### Error: File Not Found

```
❌ File not found: leads.xlsx
```

**Solution:** Check file path, use absolute path if needed

---

## CSV/Excel File Requirements

### Required Columns (must map via fuzzy matching)

1. **Business Name** - Maps to `business_legal_name`
   - Accepted headers: "Business Name", "Legal Name", "Company Name", "Merchant Name"
   
2. **Source** - Maps to `source`
   - Note: If not in file, use `--source` CLI argument

### Recommended Columns

- Phone numbers: "Phone", "Business Phone", "Telephone"
- Email: "Email", "Business Email", "Contact Email"
- Address: "Address", "Street Address", "Business Address"
- City: "City", "Business City"
- State: "State", "ST", "Business State"
- ZIP: "ZIP", "Zipcode", "Postal Code"
- Owner: "Owner", "Borrower", "Contact Name", "Principal"

### Extra Columns

Any column not recognized by the fuzzy mapper will be:
- Preserved in the `lead_appendix_kv` table
- Linked to the lead via `lead_id`
- Tagged with `upload_tag` for tracking

**Example extra columns:**
- "Offer Details"
- "Bank Name"
- "Notes"
- "Campaign ID"
- "Sales Rep"

---

## Programmatic Usage (Python API)

You can use the modules directly in Python scripts:

```python
from pathlib import Path
from ingestion.schema_loader import Schema
from ingestion.header_mapper import HeaderMapper
from ingestion.type_normalizer import Normalizer
from ingestion.deep_cleaner import DeepCleaner
from ingestion.ingest_engine import IngestEngine

# Load schema and mappers
schema = Schema('schemas/monet/schema.json')
mapper = HeaderMapper('schemas/monet/fuzzy.json')
normalizer = Normalizer()

# Clean a file
cleaner = DeepCleaner(schema, mapper, normalizer)
leads_df, owners_df, appendix_df = cleaner.clean_file(
    'my_data.csv',
    upload_tag='manual_import_001'
)

# Preview
print(f"Cleaned {len(leads_df)} leads")
print(leads_df.head())

# Ingest to database
engine = IngestEngine('monet.sqlite', schema)
engine.ingest(leads_df, owners_df, appendix_df, 'manual_import_001', 'My Source')
engine.create_indexes()

print("✅ Done!")
```

---

## Troubleshooting

### Issue: Import Error (pandas not found)

```
❌ Import error: No module named 'pandas'
```

**Solution:**
```bash
pip install pandas openpyxl
```

### Issue: Schema File Not Found

```
❌ File not found: schemas/monet/schema.json
```

**Solution:** Make sure you're running from the `octophi_ingestion/` directory:
```bash
cd octophi_ingestion
python -m ingestion.cli ...
```

### Issue: No Headers Mapped

```
✓ Mapped 0 known fields
📦 Found 27 extra columns (will store in appendix): ...
```

**Solution:** Your CSV headers don't match any fuzzy mappings. Either:
1. Rename your CSV headers to match common variants
2. Add your headers to `schemas/monet/fuzzy.json`

**Example:** Add to fuzzy.json:
```json
{
  "business_legal_name": [
    "business name",
    "YOUR_CUSTOM_HEADER_HERE"
  ]
}
```

### Issue: Database Locked

```
❌ database is locked
```

**Solution:** Close any programs accessing the database (e.g., DB Browser for SQLite)

---

## Integration with Existing System

### Using with `unified_database_migrated.sqlite`

```bash
# Run from staging_leads_logic directory
cd octophi_ingestion

python -m ingestion.cli \
    --schema monet \
    --file ../staging_leads_template.xlsx \
    --db ../unified_database_migrated.sqlite \
    --source "new_source_name"
```

This will:
- Use the existing database structure
- Add records to existing `leads`, `owners`, `sources` tables
- Maintain all foreign key relationships
- Preserve all existing data

### Comparison with `ing_logic.py`

| Feature | OCTOPHI | ing_logic.py |
|---------|---------|--------------|
| **Header Mapping** | Fuzzy matching | Exact match required |
| **Schema Definition** | JSON files | Hardcoded |
| **Normalization** | Type-specific | Template-based |
| **Extra Columns** | Automatic appendix | Automatic appendix |
| **Multi-Domain** | Yes (add schemas) | No |
| **Testing** | Built-in tests | None |

**When to use OCTOPHI:**
- Messy CSV files with inconsistent headers
- Multiple data sources with different formats
- Need for flexible schema changes
- Long-term maintainability

**When to use ing_logic.py:**
- Existing pipeline already working
- Template files already standardized
- Quick one-off imports

---

## Best Practices

### 1. Test with Dry Run First

Always test new data sources with `--dry-run`:
```bash
python -m ingestion.cli --schema monet --file new_source.csv --db test.sqlite --source "Test" --dry-run
```

### 2. Use Descriptive Source Names

Good: `"XYZ_Broker_Monthly_Feed"`  
Bad: `"data1"`

### 3. Tag Large Batches

For files >10K rows, use custom tags:
```bash
--upload-tag "2025_12_large_import_batch_1"
```

### 4. Create Indexes After Bulk Imports

```bash
# Import without indexes (faster)
python -m ingestion.cli ... --source "Source1"
python -m ingestion.cli ... --source "Source2"

# Create indexes once at the end
python -m ingestion.cli ... --source "Source3" --create-indexes
```

### 5. Keep Logs for Large Imports

```bash
python -m ingestion.cli ... --log-file logs/import_$(date +%Y%m%d).log
```

---

## Next Steps

1. ✅ Verify setup with `python verify_setup.py`
2. ✅ Test with sample data using `--dry-run`
3. ✅ Run first real import
4. ✅ Query database to verify results
5. ✅ Add custom headers to `fuzzy.json` as needed
6. ✅ Integrate into production pipeline

---

## Support

For questions or issues, refer to:
- `README.md` - System overview
- `docs/OCTOPHI_INGESTION_V1_SPEC.md` - Technical specification
- Test files in `tests/` - Usage examples

**Built with ❤️ by Otto @ Octophi**




