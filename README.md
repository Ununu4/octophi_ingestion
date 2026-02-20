# 🐙 OCTOPHI Ingestion System

**Schema-driven ETL for Monet (MCA) — Postgres, template-only**

Version: 2.0 (Postgres)  
Author: Otto / Octophi

---

## Overview (Current: Postgres + Template-Only)

- **Target database:** PostgreSQL (`leadpool_migtest`). Uses `DATABASE_URL` or `--db-url`, else `config.DEFAULT_DATABASE_URL` (`postgresql://...@localhost:5433/leadpool_migtest`).
- **Header mapping:** **Template-only** — a CSV mapping (`incoming_schema`, `expected_schema`) is **required** (`--mapping-template`). Fuzzy mode has been removed.
- Loads **CSV/XLSX/XLS**, applies **type normalization** and **derived fields**, supports **template combinations** (e.g. `first name + last name` → `owner_name`), and writes to **sources**, **leads**, **owners**, and **lead_appendix_kv**.
- One transaction; bulk inserts; source resolved by `upper(name)`.

---

## Directory Structure

```
octophi_ingestion/
├── schemas/monet/
│   └── schema.json          # Canonical schema
├── ingestion/
│   ├── cli.py               # CLI (Postgres, required template)
│   ├── schema_loader.py     # Schema parsing
│   ├── template_mapper.py   # Template-based header mapping
│   ├── type_normalizer.py   # Type normalization
│   ├── deep_cleaner.py      # Cleaning pipeline
│   └── postgres_ingest_engine.py  # Postgres bulk ingest
├── utils/ (logger, file_ops)
├── templates/               # CSV mapping templates (required)
└── tests/
```

---

## Quick Start

### 1. Installation

```bash
pip install -r requirements.txt
# pandas, openpyxl, psycopg2-binary
```

### 2. Basic Usage

Set `DATABASE_URL` (or use default `config.DEFAULT_DATABASE_URL` for `leadpool_migtest`), then run with a **required** mapping template:

```bash
# Optional: export DATABASE_URL="postgresql://postgres:postgres@localhost:5433/leadpool_migtest"

python -m ingestion.cli \
  --schema monet \
  --file /path/to/file.xlsx \
  --source "Some Vendor" \
  --mapping-template templates/some_vendor.csv
```

Dry-run (no DB writes):

```bash
python -m ingestion.cli \
  --schema monet \
  --file /path/to/file.xlsx \
  --source "Some Vendor" \
  --mapping-template templates/some_vendor.csv \
  --dry-run
```

Optional: `--db-url` to override `DATABASE_URL`; `--upload-tag`, `--log-file`, `--skip-appendix`.

Database connection: `--db-url` > `DATABASE_URL` env > `config.DEFAULT_DATABASE_URL` (leadpool_migtest).

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
| `--db-url` | No | Postgres URL (default: `DATABASE_URL` env, else `config.DEFAULT_DATABASE_URL`) |
| `--source` | Yes | Source name for this data |
| `--mapping-template` | **Yes** | Path to CSV template: `incoming_schema`, `expected_schema` |
| `--upload-tag` | No | Custom upload tag (default: timestamp) |
| `--log-file` | No | Log to file (default: console only) |
| `--dry-run` | No | Preview only, don't insert data |
| `--skip-appendix` | No | Skip appendix data insertion (only mapped fields) |

### 2.2 Preprocessing Mode (Template-Based Mapping)

**For recurring data sources with consistent headers**, use template-based preprocessing for:
- ⚡ **Lightning-fast** direct lookup (no fuzzy overhead)
- 🎯 **100% accuracy** from explicit mappings
- 📝 **Reusable** templates for each source

**Create a template CSV** (columns: `incoming_schema`, `expected_schema`):
```csv
incoming_schema,expected_schema
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




