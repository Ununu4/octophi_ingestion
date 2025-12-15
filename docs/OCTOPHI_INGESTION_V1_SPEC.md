# 🐙 OCTOPHI INGESTION SYSTEM — V1 SPECIFICATION
Modular Schema-Driven ETL + Normalization Pipeline for Monet (MCA Domain)  
Version: 1.0  
Author: Otto / Octophi  
Audience: Cursor LLM / Future Implementers

---

## 1. PURPOSE OF THIS DOCUMENT

This document defines the exact steps, architecture, and deliverables needed to build the schema-driven ingestion engine for the Monet (MCA) domain.

The goal is to:

- Create the **Monet meta-schema (`schema.json`)**
- Create the **Monet fuzzy logic mapping (`fuzzy.json`)**
- Build the **Deep Cleaning Module**
- Build the **Header Canonicalization Engine**
- Support **appendix KV ingestion**
- Modify ingestion scripts to become **fully schema-driven**
- Produce **Monet-ready cleaned CSV templates**
- Provide a **CLI to run ingestion end-to-end**

This is v1 of a universal engine that later will support multiple domains (pharmacy, real estate, etc.) simply by adding new schemas.

---

## 2. DIRECTORY LAYOUT

Create this structure:

octophi_ingestion/
    schemas/
        monet/
            schema.json
            fuzzy.json

    ingestion/
        deep_cleaner.py
        header_mapper.py
        schema_loader.py
        type_normalizer.py
        appendix_manager.py
        ingest_engine.py
        cli.py

    utils/
        logger.py
        file_ops.py

    docs/
        OCTOPHI_INGESTION_V1_SPEC.md

    tests/
        test_schema_loader.py
        test_header_mapper.py
        test_normalization.py

---

## 3. META-SCHEMA REQUIREMENTS (`schema.json`)

This file defines the canonical Monet schema including:

- Entities
- Fields
- Types
- Required flags
- Derived fields
- Relationships
- Appendix behavior

Cursor must generate:

schemas/monet/schema.json

with content:

{
  "schema_name": "monet_merchant",
  "version": "1.0",

  "entities": {
    "lead": {
      "primary_key": "id",

      "fields": {
        "business_legal_name": { "type": "string", "required": true },
        "dba": { "type": "string", "required": false },

        "business_address": { "type": "address" },
        "business_city": { "type": "string" },
        "business_state": { "type": "state" },
        "zip": { "type": "zip" },

        "phone_raw": { "type": "phone" },
        "phone_clean": { "type": "phone_clean", "derived_from": "phone_raw" },

        "start_date": { "type": "date" },
        "tax_id": { "type": "id_number" },

        "sic": { "type": "sic_code" },
        "soc": { "type": "soc_code", "derived_from": "sic" },

        "business_email": { "type": "email" },
        "business_second_email": { "type": "email" },

        "second_phone_raw": { "type": "phone" },
        "second_phone_clean": { "type": "phone_clean", "derived_from": "second_phone_raw" },

        "source": { "type": "string", "required": true },

        "created_at": { "type": "datetime", "system_generated": true },
        "updated_at": { "type": "datetime", "system_generated": true }
      }
    },

    "owner": {
      "primary_key": "id",

      "fields": {
        "owner_name": { "type": "person_name" },
        "dob": { "type": "date" },
        "ssn": { "type": "id_number" },

        "owner_address": { "type": "address" },
        "owner_city": { "type": "string" },
        "owner_state": { "type": "state" },
        "owner_zip": { "type": "zip" },

        "owner_email": { "type": "email" },
        "owner_second_email": { "type": "email" },

        "owner_phone": { "type": "phone" },
        "owner_phone_clean": { "type": "phone_clean", "derived_from": "owner_phone" },

        "created_at": { "type": "datetime", "system_generated": true },
        "updated_at": { "type": "datetime", "system_generated": true }
      }
    }
  },

  "relationships": {
    "lead_to_owner": {
      "type": "one_to_one",
      "foreign_key": "lead_id"
    }
  },

  "appendix": {
    "enabled": true,
    "table_name": "lead_appendix_kv",
    "mode": "key_value"
  }
}

This `schema.json` MUST be loaded dynamically by the ingestion engine.

---

## 4. FUZZY LOGIC FILE (`fuzzy.json`)

This file maps messy human headers → canonical schema fields.

Cursor must generate:

schemas/monet/fuzzy.json

with content:

{
  "business_legal_name": [
    "business name", "legal name", "company name", "merchant name",
    "biz name", "company", "legal business name"
  ],
  "dba": [
    "doing business as", "trade name", "fictitious name", "store name"
  ],
  "business_address": ["address", "street address", "business address", "street"],
  "business_city": ["city", "business city"],
  "business_state": ["state", "st", "business state"],
  "zip": ["zip", "zipcode", "postal", "postal code"],

  "phone_raw": [
    "phone", "phone number", "telephone", "tel", "business phone"
  ],

  "start_date": ["start date", "established", "opening date", "start"],
  "tax_id": ["ein", "tin", "federal id", "tax id", "ss4"],

  "sic": ["sic", "industry", "industry code"],
  "soc": ["soc", "vertical"],

  "business_email": ["email", "business email", "contact email"],
  "business_second_email": ["secondary email", "alt email"],

  "owner_name": ["owner", "borrower", "contact name", "principal"],
  "dob": ["dob", "birthdate", "date of birth"],
  "ssn": ["ssn", "social", "social security"],

  "owner_phone": ["owner phone", "mobile", "cell", "contact phone"],
  "owner_email": ["owner email", "personal email"]
}

This will be used by the header mapper.

---

## 5. MODULE 1 — `schema_loader.py`

**Responsibilities:**

- Load `schema.json`
- Parse field types
- Parse required fields
- Parse derived fields
- Provide:
  - `.fields(entity)`
  - `.field_type(entity, field)`
  - `.derived_from(entity, field)`

### Interface

class Schema:
    def __init__(self, path: str):
        ...
    def fields(self, entity: str) -> list[str]:
        ...
    def field_type(self, entity: str, field: str) -> str:
        ...
    def derived_from(self, entity: str, field: str) -> str | None:
        ...

---

## 6. MODULE 2 — `header_mapper.py`

**Responsibilities:**

- Load `fuzzy.json`
- Normalize raw headers (lowercase, strip, remove spaces/symbols)
- Fuzzy match headers → canonical fields
- Detect unknown headers → mark as appendix

### Interface

class HeaderMapper:
    def __init__(self, fuzzy_map_path: str):
        ...
    def map_headers(self, raw_headers: list[str]) -> dict[str, str]:
        """Return mapping: raw_header -> canonical_field (or None if appendix)"""
        ...

---

## 7. MODULE 3 — `type_normalizer.py`

**Responsibilities:**

Apply normalization based on field type, including:

- `phone` → digits only
- `phone_clean` → cleaned & formatted from `phone`
- `zip` → first 5 digits
- `state` → uppercase 2-letter
- `email` → lowercase
- `id_number` → digits only
- `date` → best-effort ISO format
- `string` → stripped
- `sic_code` → preserve but ensure digits
- `soc_code` → derived from `sic_code` (placeholder mapping for now)

### Interface

class Normalizer:
    def normalize(self, value: str, type_name: str) -> str:
        ...

`normalize` should be safe for None/empty strings.

---

## 8. MODULE 4 — `deep_cleaner.py`

**Responsibilities:**

- Take raw CSV/XLSX file path
- Read into pandas DataFrame
- Use `HeaderMapper` to canonicalize headers
- Use `Schema` + `Normalizer` to:
  - enforce schema columns
  - normalize values per type
  - handle missing fields (fill with None/empty)
- Identify extra/unmapped columns:
  - keep them in a separate structure for appendix handling
- Output:
  - A DataFrame with **exact schema fields** for:
    - `lead` entity
    - `owner` entity
  - A DataFrame with:
    - `lead_id` placeholder (row index)
    - `column_name`
    - `value`
    - `original_row_number`
    - `upload_tag`

### Interface

class DeepCleaner:
    def __init__(self, schema: Schema, mapper: HeaderMapper, normalizer: Normalizer):
        ...
    def clean_file(self, input_path: str, upload_tag: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Return (clean_leads_df, appendix_df)"""
        ...

---

## 9. MODULE 5 — `ingest_engine.py`

**Responsibilities:**

- Connect to existing Monet SQLite database (for now)
- Create or verify tables:
  - `leads`
  - `owners`
  - `lead_appendix_kv`
- Insert cleaned leads and owners into DB
- Insert appendix rows into `lead_appendix_kv`
- Use schema.json to determine:
  - which fields go to `leads`
  - which fields go to `owners`

### Interface

class IngestEngine:
    def __init__(self, db_path: str, schema: Schema):
        ...
    def ingest(self, leads_df: pd.DataFrame, appendix_df: pd.DataFrame, upload_tag: str, source_name: str):
        ...
        # 1. insert source into `sources` table (if not exists)
        # 2. insert leads
        # 3. insert owners (one per lead)
        # 4. insert appendix rows

---

## 10. MODULE 6 — `cli.py`

Create a CLI that exposes a single main command:

octophi_ingest --schema monet --file /path/to/input.xlsx --db /path/to/monet.sqlite --upload-tag 2025_12_08_monet_run --source-name "Some Broker List"

**Steps:**

1. Load schema.json (Monet)
2. Load fuzzy.json
3. Initialize:
   - `Schema`
   - `HeaderMapper`
   - `Normalizer`
   - `DeepCleaner`
4. Run `DeepCleaner.clean_file(...)`
5. Initialize `IngestEngine`
6. Call `IngestEngine.ingest(...)`
7. Print a summary:
   - number of leads ingested
   - number of appendix rows
   - any rows skipped (if validation fails)

---

## 11. REQUIRED BEHAVIOR

- MUST support Monet schema end-to-end.
- MUST load schema dynamically from `schemas/monet/schema.json`.
- MUST load fuzzy logic dynamically from `schemas/monet/fuzzy.json`.
- MUST detect unknown headers and route them to appendix.
- MUST normalize all values based on defined types.
- MUST avoid hardcoding field names inside ingestion logic.
- MUST be capable of running entirely from CLI.
- MUST be written in clean, modular Python 3.

---

## 12. IMPLEMENTATION ORDER (FOR CURSOR)

1. Scaffold directory structure.
2. Implement `schema_loader.py`.
3. Implement `header_mapper.py`.
4. Implement `type_normalizer.py`.
5. Implement `deep_cleaner.py`.
6. Implement `ingest_engine.py` (using existing Monet SQLite DB schema).
7. Implement `cli.py`.
8. Test with:
   - a small sample input.csv/xlsx
   - simulated messy headers
9. Adjust fuzzy.json mappings as needed.

---

## 13. FUTURE EXTENSIONS (NOT REQUIRED NOW, JUST DESIGN-FRIENDLY)

- Add support for other schemas: `pharmacy`, `real_estate`, etc.
- Allow `--schema` flag to choose different meta-schemas.
- Allow output of a "cleaned template" file for partners to follow.
- Plug this pipeline into a Streamlit or web UI for drag-and-drop ingestion.

---

**End of OCTOPHI_INGESTION_V1_SPEC**