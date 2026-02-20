# OCTOPHI Ingestion System — Structure and Behavior

This document describes how the OCTOPHI ingestion application works end-to-end. It is intended to be complete and accurate so that an LLM or developer can understand every feature and code path as implemented.

---

## 1. Overview

**OCTOPHI** is a schema-driven ETL pipeline for the Monet (MCA) domain. It:

- Reads **CSV** or **XLSX/XLS** files with potentially messy or inconsistent headers.
- Maps input columns to a **canonical schema** (either by **fuzzy matching** or by a **template**).
- Applies **type-specific normalization** and **derived fields** (e.g. `phone_clean` from `phone_raw`).
- Supports **field combinations** (e.g. first name + last name → `owner_name`) and **computations** (e.g. “time in business” duration → `start_date`).
- Writes normalized **leads** and **owners** to SQLite, plus **appendix** (key-value) rows for unmapped columns.
- Can **create indexes** after ingestion and supports **dry-run** (no DB writes).

The app is **modular**: schema and mapping rules live in JSON (and optional CSV template); the code is in `ingestion/` and `utils/`.

---

## 2. Directory and File Layout

```
octophi_ingestion/
├── schemas/
│   └── monet/
│       ├── schema.json           # Canonical schema: entities, fields, types, derived, required
│       ├── fuzzy.json            # Header variants → canonical field (fuzzy mode)
│       ├── fuzzy_combinations.json   # Rules to combine columns (fuzzy mode)
│       └── fuzzy_computations.json   # Rules to compute fields, e.g. duration→date (fuzzy mode)
├── ingestion/
│   ├── cli.py                    # Entry point: argparse, DB selection, orchestration
│   ├── schema_loader.py          # Loads schema.json, exposes field metadata
│   ├── header_mapper.py          # Fuzzy header mapping + combination/computation detection
│   ├── template_mapper.py       # Template-based mapping (CSV) + combinations
│   ├── type_normalizer.py       # Type-specific normalization and placeholders
│   ├── duration_parser.py       # Duration string → date (e.g. "10 years" → start date)
│   ├── deep_cleaner.py          # Main cleaning pipeline: load → map → combine → compute → entities → appendix
│   └── ingest_engine.py         # SQLite: create tables, source, leads, owners, appendix, indexes
├── utils/
│   ├── logger.py                # setup_logger (console + optional file)
│   └── file_ops.py              # validate_file_path, get_file_size_mb
├── templates/
│   ├── README.md                # How to create and use mapping templates
│   ├── MASTER_TEMPLATE.csv      # Reference template (expected_schema pre-filled)
│   └── *.csv                    # Per-source templates (incoming_schema → expected_schema)
├── tests/                       # Unit tests (schema, header mapper, normalization)
└── structure.md                 # This file
```

---

## 3. Entry Point and CLI

**Entry point:** `python -m ingestion.cli`

### 3.1 Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--schema` | Yes | Schema name (e.g. `monet`). Resolves to `schemas/<schema>/schema.json` and same-dir fuzzy/template assets. |
| `--file` | Yes | Input file path (CSV or XLSX/XLS). |
| `--source` | Yes | Source name for this data (stored in `sources` table and used for `source_id` on leads). |
| `--db` | No | SQLite database path. If omitted, **database selection is interactive** (see below). |
| `--upload-tag` | No | Tag for this batch (default: `YYYYmmdd_HHMMSS`). Stored in appendix and used for traceability. |
| `--mapping-template` | No | Path to CSV template for **template-based** header mapping. If set, **fuzzy matching is not used**. |
| `--create-indexes` | No | After ingestion, create performance indexes on leads, owners, and appendix. |
| `--log-file` | No | If set, also log to this file (in addition to console). |
| `--dry-run` | No | Run validation and cleaning only; print preview of first 5 leads/owners; **do not insert** into DB. |
| `--skip-appendix` | No | Do not insert appendix rows; only mapped (lead/owner) data is written. |

### 3.2 Database Selection (when `--db` is omitted)

The CLI calls `select_database()` which prompts:

- **[1] Current Database (db1):** `C:\Users\ottog\Desktop\monet_database_engine\unified_database_migrated.sqlite`
- **[2] New Database (db2):** `C:\Users\ottog\Desktop\monet_database_engine\unified_database_migrated_2.sqlite`

User must enter `1` or `2`; the chosen path is used as `db_path` for the rest of the run.

### 3.3 High-Level Flow (CLI)

1. Resolve `db_path` (from `--db` or interactive selection).
2. Set `upload_tag` to `--upload-tag` or current timestamp.
3. Validate input file (exists, extension in `['.csv', '.xlsx', '.xls']`) via `utils.file_ops.validate_file_path`.
4. Load schema: `Schema(schemas/<schema>/schema.json)`.
5. **If `--mapping-template` is set:** instantiate `TemplateMapper(template_path)` (**preprocessing / template mode**).
6. **Else:** instantiate `HeaderMapper(fuzzy.json)`; it auto-loads same-dir `fuzzy_combinations.json` and `fuzzy_computations.json` (**fuzzy mode**).
7. Create `Normalizer()` and `DeepCleaner(schema, mapper, normalizer)`.
8. Run `cleaner.clean_file(file_path, upload_tag)` → `(leads_df, owners_df, appendix_df)`.
9. If `--skip-appendix`: replace `appendix_df` with empty DataFrame.
10. Run `cleaner.validate_required_fields(leads_df, owners_df)`; on any errors, log and exit(1).
11. Print cleaning summary (row counts for leads, owners, appendix).
12. **If `--dry-run`:** print preview of first 5 leads and first 5 owners; exit(0).
13. Else: create `IngestEngine(db_path, schema)`, call `engine.ingest(leads_df, owners_df, appendix_df, upload_tag, source_name)`.
14. If `--create-indexes`: call `engine.create_indexes()`.
15. Print final “INGESTION COMPLETE” summary.

---

## 4. Schema System (`schema.json` and `schema_loader.py`)

### 4.1 Schema File Shape

- **Top-level:** `schema_name`, `version`, `entities`, `relationships`, `appendix`.
- **`entities`:** One key per entity, e.g. `lead`, `owner`. Each entity has `primary_key` and `fields`.
- **`fields`:** Map of field name → `{ "type", "required?", "derived_from?", "system_generated?" }`.
- **`appendix`:** `enabled` (bool), `table_name` (e.g. `lead_appendix_kv`).

### 4.2 Field Metadata (Schema class)

- **`fields(entity)`** — List of field names for that entity (from schema).
- **`field_type(entity, field)`** — Type string (e.g. `string`, `phone`, `email`, `date`). Used by the normalizer.
- **`derived_from(entity, field)`** — If the field is derived, returns source field name; else `None`.
- **`is_required(entity, field)`** — Whether the field is required (validated after cleaning; `source` is special-cased and skipped).
- **`is_system_generated(entity, field)`** — If true, field is excluded from entity DataFrame construction and from insert columns (e.g. `created_at`, `updated_at`).
- **`appendix_enabled()`** / **`appendix_table_name()`** — From `appendix` in schema.

### 4.3 Monet Schema Entities (as of current `schema.json`)

**Lead:** `business_legal_name` (required), `dba`, `business_address`, `business_city`, `business_state`, `zip`, `phone_raw`, `phone_clean` (derived from `phone_raw`), `start_date`, `tax_id`, `soc`, `business_email`, `business_second_email`, `second_phone_raw`, `second_phone_clean` (derived from `second_phone_raw`), `created_at`, `updated_at` (system-generated).  
Note: `source` is not in the schema; it is provided by CLI and stored as `source_id` on the lead row at ingest time.

**Owner:** `owner_name`, `dob`, `ssn`, `owner_address`, `owner_city`, `owner_state`, `owner_zip`, `owner_email`, `owner_second_email`, `owner_phone`, `owner_phone_clean` (derived from `owner_phone`), `created_at`, `updated_at` (system-generated).

**Relationship:** Lead-to-owner is one-to-one; `owners.lead_id` references `leads.id`.

---

## 5. Header Mapping: Two Modes

The cleaner uses a single **mapper** object that implements:

- `map_headers(raw_headers: List[str]) -> Dict[str, Optional[str]]` — raw header → canonical field or `None`.
- `get_combinations()` — list of combination rules (used by DeepCleaner).
- `get_computations()` — list of computation rules (fuzzy mode only; template returns `[]`).

The mapper is either a **TemplateMapper** or a **HeaderMapper** (fuzzy). DeepCleaner branches on `mapper.mapper_type == 'template'`.

---

### 5.1 Fuzzy Mode (`HeaderMapper`)

- **Input:** Path to `fuzzy.json`. By default, same directory is used for `fuzzy_combinations.json` and `fuzzy_computations.json`.
- **`fuzzy.json`:** Map from **canonical field name** to list of **header variants** (e.g. `"business_legal_name": ["business name", "legal name", "company name", ...]`). Used to build a reverse map: normalized variant → canonical field.

**Header normalization (for matching):**

- Lowercase, strip, replace `_\-\./\\` with space, remove non-alphanumeric (except spaces), collapse spaces, then remove all spaces. So `"Business Name"` and `"business name"` both match the same variant.

**`map_headers(raw_headers)`:**

- For each raw header, normalize and look up in the reverse map. If found → canonical field; else → `None`.
- Then **combination detection** and **computation detection** run; they can **overwrite** the mapping for some headers to internal markers (e.g. `_COMBINE_<target>`, `_COMPUTE_<target>`).

**Combinations (`fuzzy_combinations.json`):**

- Structure: `{ "combinations": [ { "target_field", "type", "sources": [ {"pattern", "order"}, ... ], "separator", "description" } ] }`.
- For each rule, the mapper checks if **every** source pattern (after normalization) matches some raw header. If so, it records a combination match with the **actual** column names (in order) and sets each of those headers’ mapping to `_COMBINE_<target_field>` so they are not treated as appendix.
- Example: first name + last name → `owner_name` with separator `" "`.

**Computations (`fuzzy_computations.json`):**

- Structure: `{ "computations": [ { "target_field", "type", "sources": [ {"pattern"}, ... ], "computation", "description" } ] }`.
- For each rule, the mapper finds **one** raw header that matches **any** of the source patterns. If found, it records a computation match (source column, target field, type) and sets that header’s mapping to `_COMPUTE_<target_field>`.
- Example: "tib" / "time in business" / "years in business" → `start_date` with type `duration_to_date`.

---

### 5.2 Template Mode (`TemplateMapper`)

- **Input:** Path to a CSV file with columns **`incoming_schema`** and **`expected_schema`**.
- Rows are read; for each row, `incoming_schema` is normalized (lowercase, strip) and mapped to `expected_schema`.
- **Combination syntax:** If `incoming_schema` contains `" + "` (e.g. `"first name + last name"`), it is parsed as a combination: sources = split by `+` and strip; target = `expected_schema`. Those source names are stored as “used in combination” and mapped to `__USED_IN_COMBINATION__` so they do not go to appendix. Separator is space.
- **No computations** in template mode: `get_computations()` returns `[]`.

Mapping is **O(1) per header**; no fuzzy logic. Unmapped columns remain as `None` in the mapping and are treated as appendix (unless excluded by name).

---

## 6. DeepCleaner Pipeline (`deep_cleaner.py`)

### 6.1 Input Loading (`_load_file`)

- **Supported extensions:** `.csv`, `.xlsx`, `.xls`.
- Pandas: `dtype=str`, `keep_default_na=False` so that blanks stay as strings and are not turned into NaN.

### 6.2 Header Mapping and Mode Branch

- `header_mapping = mapper.map_headers(df.columns.tolist())`.
- **If template mode:** apply combinations (if any), then determine **known** vs **unknown** columns. Known = mapping value not `None` and not `__USED_IN_COMBINATION__`. Unknown = mapping `None`. Combination source columns are removed from unknown. Columns in `exclude_from_appendix` (e.g. `ZB Status`, `zb status`, `ZB Status `) are removed from unknown.
- **If fuzzy mode:** apply combinations, then **computations**. Known = mapping not `None` and not starting with `_COMBINE_`. Unknown = mapping `None` or starting with `_COMBINE_`; then remove combination and computation source columns and exclude_from_appendix.

### 6.3 Rename and Entity DataFrames

- DataFrame is renamed so that **known** columns use canonical names: `df_renamed = df.rename(columns=known_cols)`.
- **Lead** and **owner** entity DataFrames are built by `_create_entity_df(df_renamed, field_list, entity)` for schema fields of `lead` and `owner`.

### 6.4 Creating Entity Rows (`_create_entity_df`)

- For each schema field (skipping system-generated):
  - If the field is **derived** (`derived_from` set):
    - If source column exists: derive value (e.g. `phone_clean` from `phone_raw` via `normalizer.derive_phone_clean`; `soc` from `sic` via `normalizer.derive_soc_from_sic`; others by normalizing with target type).
    - Else: fill with `None`.
  - Else:
    - If column exists: **special case** for `start_date` with type `date`: values are passed through `_convert_tib_to_date` (numeric “years in business” → start date as `(current_year - value)-01-01`; otherwise treat as date string and normalize).
    - Otherwise normalize with `schema.field_type(entity, field)`.
  - If the field is not present in the renamed DataFrame: fill with `None`.

### 6.5 Time-in-Business to Date (`_convert_tib_to_date`)

- If value looks like a date (contains `-` or `/`), it is normalized as a date.
- Else, if it parses as a number in [0, 100], it is treated as years in business: `start_date = (current_year - value)-01-01`.
- Otherwise the value is passed to the date normalizer.

### 6.6 Appendix DataFrame (`_create_appendix_df`)

- **Input:** Original (unrenamed) DataFrame, list of **extra_columns** (unknown/appendix column names), and `upload_tag`.
- For each row index and each extra column, if the cell value is non-empty (after strip), append a row: `lead_id_placeholder` = row index (integer), `column_name` = column name, `value` = stripped string, `original_row_number` = 1-based index, `upload_tag`.
- Used later to insert into `lead_appendix_kv` after replacing `lead_id_placeholder` with actual `lead_id`s.

### 6.7 Combinations and Computations (inside DeepCleaner)

- **`_apply_combinations(df, combinations, header_mapping)`:** For each combination rule, find actual column names (case-insensitive match). Build a new column: concatenate source columns with the rule’s separator, strip. Add that column to the DataFrame and set `header_mapping[target_field] = target_field`.
- **`_apply_computations(df, computations, header_mapping)`:** For each computation of type `duration_to_date`, apply `DurationParser.parse_duration_to_date` to the source column and assign to `target_field`; set `header_mapping[target_field] = target_field`.

### 6.8 Validation (`validate_required_fields`)

- For each required field of lead (and owner), if the field is `source` it is skipped (provided by CLI). Otherwise: field must exist in the entity DataFrame and not be all null/empty. Returns a list of error messages; non-empty means validation failed.

---

## 7. Type Normalizer (`type_normalizer.py`)

### 7.1 Placeholders

Values that normalize to **None**: `na`, `n/a`, `none`, `null`, `nil`, `unknown`, `unspecified`, `tbd`, `nan`, empty string (case-insensitive).

### 7.2 Main Entry: `normalize(value, type_name)`

- None or empty → None. Then stringify and strip. If placeholder → None. Else dispatch to `_normalize_<type_name>` if present; otherwise `_normalize_string`.

### 7.3 Type-Specific Behavior

| Type | Behavior |
|------|----------|
| `string` | Strip; return None if empty. |
| `phone` / `phone_clean` | Digits only (remove non-digits). |
| `zip` | Digits only; take first 5. |
| `state` | Strip, uppercase; if length 2 and alpha, return (e.g. CA). |
| `email` | Lowercase; must contain `@` and `.` else None. |
| `id_number` | Digits only. |
| `date` | Try several formats (e.g. `%Y-%m-%d`, `%m/%d/%Y`, `%d/%m/%Y`, `%Y%m%d`, …); return `YYYY-MM-DD` or None. |
| `datetime` | ISO-like or common formats → `YYYY-MM-DD HH:MM:SS`. |
| `sic_code` | Strip; digits extracted or original. |
| `soc_code` | Strip. |
| `address` | Same as string. |
| `person_name` | Strip; collapse multiple spaces. |

### 7.4 Derived Helpers

- **`derive_phone_clean(phone_raw)`** — Digits only from raw phone.
- **`derive_soc_from_sic(sic)`** — If SIC is in internal `SIC_TO_SOC_MAP`, return mapped value; else pass through SIC. (Map is currently empty in code.)

---

## 8. Duration Parser (`duration_parser.py`)

**`DurationParser.parse_duration_to_date(duration_str, reference_date=None)`**

- **Reference:** Default is today.
- Placeholders (na, n/a, none, null, unknown, '') → None.
- **Years:** Regex for `(\d+)\s*(?:\+)?\s*(?:year|yr)` → subtract that many years (365 days each) from reference.
- **Months:** Regex for `(\d+)\s*(?:\+)?\s*(?:month|mo)` → subtract months × 30 days.
- **Bare number:** If a number is found, if > 12 treat as months (× 30 days), else years (× 365 days).
- Returns date string `YYYY-MM-DD` or None.

Used by **fuzzy computations** when type is `duration_to_date` (e.g. “time in business” → `start_date`).

---

## 9. Ingest Engine (`ingest_engine.py`)

- **Database:** SQLite only. If the file does not exist, a message is printed and a new DB is created on first write.

### 9.1 Table DDL (`_ensure_tables`)

- **sources:** `id` (PK AUTOINCREMENT), `name` (TEXT NOT NULL UNIQUE), `notes`, `created_at` (default `datetime('now')`).
- **leads:** `id`, `business_legal_name`, `dba`, `business_address`, `business_city`, `business_state`, `zip`, `phone_raw`, `phone_clean`, `start_date`, `tax_id`, `soc`, `business_email`, `business_second_email`, `second_phone_raw`, `second_phone_clean`, `source_id`, `created_at`, `updated_at`, FK `source_id` → sources(id).
- **owners:** `id`, `lead_id` (NOT NULL, UNIQUE, FK to leads ON DELETE CASCADE), then owner fields, `created_at`, `updated_at`.
- **lead_appendix_kv:** `id`, `lead_id`, `source_id`, `upload_tag`, `original_row_number`, `column_name`, `value`, `created_at`; FKs to leads and sources.

### 9.2 Ingest Flow (`ingest`)

1. **Ensure tables** exist.
2. **Ensure source:** INSERT into `sources(name)` or, on UNIQUE violation, SELECT id; set `source_id`.
3. Add column `source_id` to `leads_df`.
4. **Insert leads** row-by-row; collect `lead_ids` in order (from `cursor.lastrowid`). Insert columns = schema lead fields (excluding system-generated) + `source_id` if present.
5. **Insert owners** row-by-row; each row gets `lead_id = lead_ids[idx]`. Columns = `lead_id` + schema owner fields (excluding system-generated).
6. **Insert appendix:** For each appendix row, replace `lead_id_placeholder` (row index) with `lead_ids[placeholder]`; bulk insert into `lead_appendix_kv` with `source_id` and `upload_tag`.
7. Commit.

### 9.3 Indexes (`create_indexes`)

Created only if `--create-indexes` is passed:

- Leads: `business_state`, `zip`, `source_id`, `phone_clean`, `business_email`
- Owners: `lead_id`, `owner_phone_clean`
- Appendix: `lead_id`, `column_name`, `upload_tag`

---

## 10. Utilities

### 10.1 File Ops (`utils/file_ops.py`)

- **`validate_file_path(file_path, required_extensions)`** — Path must exist, be a file, and have suffix in the list (e.g. `['.csv', '.xlsx', '.xls']`). Returns `Path` or raises.
- **`get_file_size_mb(file_path)`** — File size in MB.

### 10.2 Logger (`utils/logger.py`)

- **`setup_logger(name, log_file=None, level)`** — Console handler always; if `log_file` is set, also file handler. Formatter includes timestamp, level, message. Used by CLI with `--log-file`.

---

## 11. Templates Folder

- **Purpose:** Store CSV templates for **template-based** header mapping (no fuzzy matching).
- **Format:** Two columns: `incoming_schema`, `expected_schema`. Headers are normalized (lowercase, strip). Combination rows use `"col1 + col2"` in `incoming_schema` and the target canonical field in `expected_schema`.
- **Usage:** Pass path via `--mapping-template`. See `templates/README.md` for how to create a template from `MASTER_TEMPLATE.csv` and run with `--dry-run` to test.

---

## 12. End-to-End Flow Summary

1. **CLI** parses args; resolves DB path (flag or interactive).
2. **Schema** and **mapper** (template or fuzzy) are loaded.
3. **DeepCleaner** loads the file (pandas, str, no default NA), maps headers, applies combinations (and in fuzzy mode computations), splits known vs appendix columns, renames, builds lead and owner DataFrames (with derived fields and TIB→date for `start_date`), builds appendix DataFrame, validates required fields.
4. If not dry-run, **IngestEngine** ensures tables, resolves source, inserts leads (get IDs), inserts owners (by index), inserts appendix (placeholder → lead_id), optionally creates indexes.
5. Summary is printed (and optionally logged to file).

This is the full structure and behavior of the ingestion app as implemented.
