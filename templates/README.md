# Preprocessing Templates

This directory contains CSV templates for preprocessing-based header mapping.

## Purpose

Templates provide explicit, 1:1 mappings between incoming CSV headers and expected schema fields, bypassing fuzzy matching for:
- **Speed**: Lightning-fast direct dictionary lookup
- **Accuracy**: No ambiguity, guaranteed correct mappings
- **Predictability**: Same file structure always maps the same way

## Master Template: `MASTER_TEMPLATE.csv`

This is your reference template with all canonical schema fields pre-filled in column B (`expected_schema`).

**Column Structure:**
- **Column A** (`incoming_schema`): **YOU FILL THIS** with actual CSV headers from your source file
- **Column B** (`expected_schema`): Canonical DB schema fields (pre-filled, DO NOT CHANGE)

## Template Format

```csv
incoming_schema,expected_schema
<your csv header 1>,business_legal_name
<your csv header 2>,owner_name
,dba
<your csv header 3>,phone_raw
...
```

**Notes:**
- Leave `incoming_schema` blank for fields not in your source CSV
- Only fill in rows where you have matching data
- Headers are case-insensitive

## Usage

```bash
python -m ingestion.cli \
  --schema monet \
  --file "data.csv" \
  --db "database.sqlite" \
  --source "MySource" \
  --mapping-template "templates/my_template.csv"
```

## Creating a Template for Your CSV

**Step 1: Analyze your source file**
```bash
# Quick preview of headers
python -c "import pandas as pd; print('\n'.join(pd.read_csv('your_file.csv', nrows=0).columns))"
```

**Step 2: Copy and fill the master template**
1. Copy `MASTER_TEMPLATE.csv` to a new file (e.g., `my_source_template.csv`)
2. Open in Excel or text editor
3. Fill column A (`incoming_schema`) with your CSV's actual headers
4. Match each to the appropriate column B field
5. Leave rows blank if field doesn't exist in your source

**Example:**
```csv
incoming_schema,expected_schema
Company Name,business_legal_name
Full Contact Name,owner_name
,dba
Business Phone,phone_raw
...
```

**Step 3: Test before ingestion**
```bash
python -m ingestion.cli \
  --schema monet \
  --file "your_file.csv" \
  --db "database.sqlite" \
  --source "TestSource" \
  --mapping-template "templates/my_source_template.csv" \
  --dry-run
```

## Available Templates

- `MASTER_TEMPLATE.csv` - **Reference template** with all schema fields
- `ucc_order_filled.csv` - Example: UCC Order files (IUSA format)

## Notes

- Headers are case-insensitive (normalized to lowercase)
- Leading/trailing whitespace is automatically stripped
- Unmapped columns automatically go to appendix (if not using `--skip-appendix`)
- Templates bypass fuzzy matching, combinations, and computations

