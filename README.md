# 🐙 OCTOPHI Ingestion System V1

**Modular Schema-Driven ETL + Normalization Pipeline for Monet (MCA Domain)**

Version: 1.0  
Author: Otto / Octophi

---

## Overview

OCTOPHI is a universal, schema-driven ingestion engine that:

- ✅ Loads data from **messy CSV/XLSX files**
- ✅ Maps fuzzy headers to **canonical schema fields**
- ✅ Applies **type-specific normalization** (phones, emails, dates, etc.)
- ✅ Handles **extra columns** via appendix table
- ✅ Produces **clean, normalized data** ready for database insertion
- ✅ Supports **multiple domains** (currently: Monet/MCA)

---

## Directory Structure

```
octophi_ingestion/
├── schemas/
│   └── monet/
│       ├── schema.json      # Canonical schema definition
│       └── fuzzy.json       # Header mapping rules
│
├── ingestion/
│   ├── schema_loader.py     # Schema parsing
│   ├── header_mapper.py     # Fuzzy header matching
│   ├── type_normalizer.py   # Type-specific cleaning
│   ├── deep_cleaner.py      # Main cleaning orchestration
│   ├── ingest_engine.py     # Database operations
│   └── cli.py               # Command-line interface
│
├── utils/
│   ├── logger.py            # Logging utilities
│   └── file_ops.py          # File operations
│
├── tests/
│   ├── test_schema_loader.py
│   ├── test_header_mapper.py
│   └── test_normalization.py
│
└── docs/
    └── OCTOPHI_INGESTION_V1_SPEC.md
```

---

## Quick Start

### 1. Installation

No external dependencies required beyond Python 3.9+ standard library and pandas:

```bash
pip install pandas openpyxl
```

### 2. Basic Usage

```bash
# Navigate to project directory
cd octophi_ingestion

# Run ingestion with interactive database selection
python -m ingestion.cli \
    --schema monet \
    --file /path/to/input.xlsx \
    --source "Broker Name"
# System will prompt you to choose between db1 (current) or db2 (new)

# Or specify database directly
python -m ingestion.cli \
    --schema monet \
    --file /path/to/input.xlsx \
    --db /path/to/database.sqlite \
    --source "Broker Name"

# With preprocessing template (recommended for recurring sources)
python -m ingestion.cli \
    --schema monet \
    --file /path/to/input.xlsx \
    --source "Broker Name" \
    --mapping-template templates/my_source_template.csv
# System will prompt for database selection
```

### 2.1 Database Selection

The system supports two production databases:

- **Database 1 (db1)**: Current database at `C:\Users\ottog\Desktop\monet_database_engine\unified_database_migrated.sqlite`
- **Database 2 (db2)**: New database at `C:\Users\ottog\Desktop\monet_database_engine\unified_database_migrated_2.sqlite`

If you don't specify `--db` flag, the system will prompt you to choose:

```
======================================================================
DATABASE SELECTION
======================================================================

Please select target database:

  [1] Current Database (db1)
      C:\Users\ottog\Desktop\monet_database_engine\unified_database_migrated.sqlite

  [2] New Database (db2)
      C:\Users\ottog\Desktop\monet_database_engine\unified_database_migrated_2.sqlite

Enter your choice (1 or 2):
```

### 3. Full Command Options

```bash
python -m ingestion.cli \
    --schema monet \
    --file input.csv \
    --db monet.sqlite \
    --source "Source Name" \
    --upload-tag "2025_12_08_batch_1" \
    --create-indexes \
    --log-file ingestion.log \
    --dry-run
```

**Arguments:**

| Argument | Required | Description |
|----------|----------|-------------|
| `--schema` | Yes | Schema name (e.g., "monet") |
| `--file` | Yes | Input CSV or XLSX file |
| `--db` | No | SQLite database path (if omitted, will prompt for db1 or db2) |
| `--source` | Yes | Source name for this data |
| `--mapping-template` | No | **Path to CSV template for preprocessing header mapping (bypasses fuzzy matching)** |
| `--upload-tag` | No | Custom upload tag (default: timestamp) |
| `--create-indexes` | No | Create performance indexes |
| `--log-file` | No | Log to file (default: console only) |
| `--dry-run` | No | Preview only, don't insert data |
| `--skip-appendix` | No | Skip appendix data insertion (only mapped fields) |

### 2.2 Preprocessing Mode (Template-Based Mapping)

**For recurring data sources with consistent headers**, use template-based preprocessing for:
- ⚡ **Lightning-fast** direct lookup (no fuzzy overhead)
- 🎯 **100% accuracy** from explicit mappings
- 📝 **Reusable** templates for each source

**Create a template CSV:**
```csv
original_header,canonical_field
Business Name,business_legal_name
Phone,phone_raw
Owner Name,owner_name
Address,business_address
```

**Use the template:**
```bash
python -m ingestion.cli \
    --schema monet \
    --file data.xlsx \
    --source "Broker Name" \
    --mapping-template templates/broker_template.csv
# Will prompt for database selection
```

**See `templates/README.md` for detailed template documentation.**

### 2.3 Using the Batch Script

For Windows users, use the convenient batch script:

```batch
REM Interactive database selection (will prompt)
ingest_to_production.bat "path\to\data.csv" "Source Name"

REM Specify database 1 (current)
ingest_to_production.bat "path\to\data.csv" "Source Name" 1

REM Specify database 2 (new)
ingest_to_production.bat "path\to\data.csv" "Source Name" 2

REM With preprocessing template (db1)
ingest_to_production.bat "path\to\data.csv" "Source Name" 1 "templates\source_template.csv"

REM With template, interactive database selection (leave db parameter empty)
ingest_to_production.bat "path\to\data.csv" "Source Name" "" "templates\source_template.csv"
```

---

## Features

### 1. Schema-Driven Architecture

All field definitions, types, and relationships are defined in `schemas/monet/schema.json`:

```json
{
  "schema_name": "monet_merchant",
  "version": "1.0",
  "entities": {
    "lead": {
      "fields": {
        "business_legal_name": { "type": "string", "required": true },
        "phone_raw": { "type": "phone" },
        "phone_clean": { "type": "phone_clean", "derived_from": "phone_raw" }
      }
    }
  }
}
```

### 2. Fuzzy Header Mapping

Maps messy CSV headers to canonical fields via `schemas/monet/fuzzy.json`:

```json
{
  "business_legal_name": [
    "business name", "legal name", "company name", "merchant name"
  ],
  "phone_raw": [
    "phone", "phone number", "telephone", "business phone"
  ]
}
```

**Examples:**
- `"Business Name"` → `business_legal_name`
- `"Ph one"` → `phone_raw`
- `"LEGAL NAME"` → `business_legal_name`

### 3. Type-Specific Normalization

Automatic normalization based on field types:

| Type | Normalization |
|------|---------------|
| `phone` | Digits only: `(555) 123-4567` → `5551234567` |
| `email` | Lowercase: `USER@EXAMPLE.COM` → `user@example.com` |
| `state` | Uppercase 2-letter: `ca` → `CA` |
| `zip` | First 5 digits: `12345-6789` → `12345` |
| `id_number` | Digits only: `123-45-6789` → `123456789` |
| `date` | ISO format: `01/15/2020` → `2020-01-15` |

### 4. Derived Fields

Automatically derive fields from source fields:

- `phone_clean` ← `phone_raw` (digits only)
- `second_phone_clean` ← `second_phone_raw`
- `owner_phone_clean` ← `owner_phone`
- `soc` ← `sic` (industry mapping)

### 5. Appendix System

Extra columns not in schema are automatically stored in `lead_appendix_kv` table:

| lead_id | column_name | value | upload_tag |
|---------|-------------|-------|------------|
| 1 | offer | $50K / 1.25 / 6mo | 20251208_101530 |
| 1 | bank | Chase | 20251208_101530 |

### 6. Placeholder Detection

Automatically converts placeholder values to `NULL`:

- `NA`, `N/A`, `NONE`, `NULL`, `NIL`
- `UNKNOWN`, `UNSPECIFIED`, `TBD`, `NAN`

---

## Schema Reference (Monet)

### Lead Entity Fields

- `business_legal_name` (required)
- `dba`
- `business_address`, `business_city`, `business_state`, `zip`
- `phone_raw`, `phone_clean` (derived)
- `second_phone_raw`, `second_phone_clean` (derived)
- `business_email`, `business_second_email`
- `start_date`
- `tax_id`
- `sic`, `soc` (derived)
- `source` (required)

### Owner Entity Fields

- `owner_name`
- `dob`
- `ssn`
- `owner_address`, `owner_city`, `owner_state`, `owner_zip`
- `owner_email`, `owner_second_email`
- `owner_phone`, `owner_phone_clean` (derived)

### Relationships

- **Lead ↔ Owner**: One-to-one (each lead has one owner)
- **Source ↔ Lead**: One-to-many (one source has many leads)

---

## Testing

### Run All Tests

```bash
# Test schema loader
python octophi_ingestion/tests/test_schema_loader.py

# Test header mapper
python octophi_ingestion/tests/test_header_mapper.py

# Test normalization
python octophi_ingestion/tests/test_normalization.py
```

### Run Individual Module Tests

```python
from ingestion.schema_loader import Schema
from ingestion.header_mapper import HeaderMapper
from ingestion.type_normalizer import Normalizer

# Test schema
schema = Schema('schemas/monet/schema.json')
print(schema.fields('lead'))

# Test mapper
mapper = HeaderMapper('schemas/monet/fuzzy.json')
print(mapper.get_canonical_field('Business Name'))

# Test normalizer
normalizer = Normalizer()
print(normalizer.normalize('(555) 123-4567', 'phone'))
```

---

## Database Schema

The system creates/uses these tables:

### `sources`
- `id` (PK)
- `name` (UNIQUE)
- `notes`
- `created_at`

### `leads`
- `id` (PK)
- `business_legal_name`, `dba`, `business_address`, ...
- `phone_raw`, `phone_clean`, `second_phone_raw`, `second_phone_clean`
- `business_email`, `business_second_email`
- `start_date`, `tax_id`, `soc`
- `source_id` (FK → sources)
- `created_at`, `updated_at`

### `owners`
- `id` (PK)
- `lead_id` (FK → leads, UNIQUE)
- `owner_name`, `dob`, `ssn`
- `owner_address`, `owner_city`, `owner_state`, `owner_zip`
- `owner_email`, `owner_second_email`
- `owner_phone`, `owner_phone_clean`
- `created_at`, `updated_at`

### `lead_appendix_kv`
- `id` (PK)
- `lead_id` (FK → leads)
- `source_id` (FK → sources)
- `upload_tag`
- `original_row_number`
- `column_name`
- `value`
- `created_at`

---

## Examples

### Example 1: Basic Ingestion

```bash
python -m ingestion.cli \
    --schema monet \
    --file broker_leads.xlsx \
    --db monet_production.sqlite \
    --source "XYZ Broker"
```

**Output:**
```
======================================================================
🐙 OCTOPHI INGESTION SYSTEM V1
======================================================================

INFO | Validating input file: broker_leads.xlsx
INFO | ✓ File validated: 2.45 MB
INFO | Loading schema: monet
INFO | ✓ Schema loaded: monet_merchant v1.0
INFO | Loading fuzzy mappings...
INFO | ✓ Fuzzy mappings loaded
INFO | Initializing deep cleaner...

INFO | 🧹 Cleaning file: broker_leads.xlsx
----------------------------------------------------------------------
📋 Loaded 500 rows with 35 columns
✓ Mapped 27 known fields
📦 Found 8 extra columns (will store in appendix): offer, bank, ...
✅ Cleaning complete: 500 leads, 500 owners, 4000 appendix rows
----------------------------------------------------------------------

📊 CLEANING SUMMARY
----------------------------------------------------------------------
Leads:         500 rows
Owners:        500 rows
Appendix:    4,000 rows
----------------------------------------------------------------------

INFO | Connecting to database: monet_production.sqlite
🚀 Starting ingestion...
   Source: XYZ Broker
   Upload tag: 20251208_143052
   Records: 500 leads, 500 owners, 4000 appendix rows
✓ Source ID: 12
✓ Inserted 500 leads
✓ Inserted 500 owners
✓ Inserted 4,000 appendix rows
✅ Ingestion complete!

======================================================================
✅ INGESTION COMPLETE
======================================================================
Source:      XYZ Broker
Upload tag:  20251208_143052
Database:    monet_production.sqlite
Leads:       500
Owners:      500
Appendix:    4,000
======================================================================
```

### Example 2: Dry Run (Preview Only)

```bash
python -m ingestion.cli \
    --schema monet \
    --file test_data.csv \
    --db monet.sqlite \
    --source "Test" \
    --dry-run
```

### Example 3: With Indexes

```bash
python -m ingestion.cli \
    --schema monet \
    --file large_feed.csv \
    --db monet.sqlite \
    --source "Big Data Feed" \
    --create-indexes
```

---

## Extending to New Domains

To add support for a new domain (e.g., "pharmacy"):

1. Create `schemas/pharmacy/schema.json`
2. Create `schemas/pharmacy/fuzzy.json`
3. Run: `python -m ingestion.cli --schema pharmacy ...`

The system automatically loads the appropriate schema!

---

## Design Principles

1. **Schema-Driven**: No hardcoded field names in code
2. **Modular**: Each component is independent and testable
3. **Extensible**: Add new domains by adding schema files
4. **Safe**: Validates data before insertion
5. **Transparent**: Clear logging at every step
6. **Flexible**: Extra columns preserved via appendix

---

## Troubleshooting

### Issue: "Schema file not found"
**Solution:** Ensure you're running from `octophi_ingestion/` directory or use absolute paths

### Issue: Required field validation fails
**Solution:** Check that CSV has `business_legal_name` and `source` columns (or fuzzy matches)

### Issue: No headers mapped
**Solution:** Add your header variants to `schemas/monet/fuzzy.json`

---

## Performance

- **Small files** (<10K rows): ~5-10 seconds
- **Medium files** (10K-100K rows): ~30-60 seconds
- **Large files** (>100K rows): ~2-5 minutes

**Optimization tips:**
- Use `--create-indexes` for large databases
- Process files in batches if >500K rows
- Use SSD for database storage

---

## Future Enhancements

- [ ] Support for PostgreSQL/MySQL
- [ ] Web UI for drag-and-drop ingestion
- [ ] Advanced SIC→SOC mapping
- [ ] Duplicate detection options
- [ ] Data quality scoring
- [ ] Support for JSON/XML input
- [ ] Streaming mode for massive files

---

## License

Internal use only - Octophi/Monet Capital

---

## Support

For questions or issues, contact the development team.

**Built with ❤️ by Otto @ Octophi**




