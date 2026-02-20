# Schema the ingest engine expects (Postgres)

This is the schema the **Postgres ingest engine** is built against. The DB must have these tables and columns; the engine does **not** create or alter tables.

---

## Connection

- **URL:** From `DATABASE_URL` env or `--db-url`.
- Example: `postgresql://postgres:***@localhost:5433/leadpool_migtest`

---

## Tables and columns (authoritative)

### `sources`

| Column      | Type   | Nullable | Notes                    |
|------------|--------|----------|--------------------------|
| id         | bigint | NO       | PK, sequence-backed      |
| name       | text   | NO       | Unique on `upper(name)`  |
| notes      | text   | YES      |                          |
| created_at | text   | —        | default `(now())::text`  |

- Unique index: `idx_sources_name_upper` on `upper(name)`.

---

### `leads`

| Column                 | Type   | Nullable | Notes        |
|------------------------|--------|----------|--------------|
| id                     | bigint | NO       | PK           |
| business_legal_name    | text   | YES      |              |
| dba                    | text   | YES      |              |
| business_address       | text   | YES      |              |
| business_city          | text   | YES      |              |
| business_state         | text   | YES      |              |
| zip                    | text   | YES      |              |
| phone_raw              | text   | YES      |              |
| phone_clean            | text   | YES      |              |
| start_date             | text   | YES      |              |
| tax_id                 | text   | YES      |              |
| soc                    | text   | YES      |              |
| source_id              | bigint | YES      | FK → sources(id) |
| created_at             | text   | —        | default      |
| updated_at             | text   | YES      |              |
| business_email        | text   | YES      |              |
| business_second_email  | text   | YES      |              |
| second_phone_raw      | text   | YES      |              |
| second_phone_clean    | text   | YES      |              |

---

### `owners`

| Column             | Type   | Nullable | Notes              |
|--------------------|--------|----------|--------------------|
| id                 | bigint | NO       | PK                 |
| lead_id            | bigint | NO       | FK → leads(id)     |
| owner_name         | text   | YES      |                    |
| dob                | text   | YES      |                    |
| ssn                | text   | YES      |                    |
| owner_address      | text   | YES      |                    |
| owner_city         | text   | YES      |                    |
| owner_state        | text   | YES      |                    |
| owner_zip          | text   | YES      |                    |
| created_at         | text   | —        | default            |
| updated_at         | text   | YES      |                    |
| owner_email        | text   | YES      |                    |
| owner_second_email | text   | YES      |                    |
| owner_phone        | text   | YES      |                    |
| owner_phone_clean  | text   | YES      |                    |

- One row per lead; `lead_id` unique.

---

### `lead_appendix_kv`

| Column              | Type   | Nullable | Notes           |
|---------------------|--------|----------|-----------------|
| id                  | bigint | NO       | PK              |
| lead_id             | bigint | NO       | FK → leads.id   |
| source_id           | bigint | YES      | FK → sources.id |
| upload_tag          | text   | YES      |                 |
| original_row_number | bigint | YES      |                 |
| column_name         | text   | NO       |                 |
| value               | text   | YES      |                 |
| created_at          | text   | —        | default         |

---

## Columns the engine inserts (order)

- **Leads:** `LEADS_INSERT_COLUMNS` in `ingestion/postgres_ingest_engine.py` (no `id`, `created_at`, `updated_at`).
- **Owners:** `OWNERS_INSERT_COLUMNS` (includes `lead_id`).
- **Appendix:** `lead_id`, `source_id`, `upload_tag`, `original_row_number`, `column_name`, `value`.

To see the **live** schema from your DB, run:

```bash
export DATABASE_URL="postgresql://..."
python show_db_schema.py
```
