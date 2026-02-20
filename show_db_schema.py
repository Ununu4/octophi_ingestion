"""
Ping the Postgres DB and output the schema (tables + columns) as JSON.
Requires: DATABASE_URL in environment, psycopg2-binary installed.

Usage:
  set DATABASE_URL=postgresql://...
  python show_db_schema.py [output.json]
  If no file given, writes db_schema.json in current directory.
"""

import json
import os
import sys

try:
    import psycopg2
except ImportError:
    print("psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

TABLES = ('sources', 'leads', 'owners', 'lead_appendix_kv')


def main():
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        print("DATABASE_URL is not set. Set it to your Postgres connection string.")
        sys.exit(1)

    try:
        conn = psycopg2.connect(db_url)
    except Exception as e:
        print(f"Could not connect to database: {e}")
        sys.exit(1)

    cur = conn.cursor()
    cur.execute("""
        SELECT table_name, column_name, data_type, is_nullable, ordinal_position
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name IN %s
        ORDER BY table_name, ordinal_position
    """, (TABLES,))
    rows = cur.fetchall()
    if not rows:
        # Fallback: list all tables in public schema for context
        cur.execute("""
            SELECT table_name, column_name, data_type, is_nullable, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
        """)
        rows = cur.fetchall()
    cur.close()
    conn.close()

    # Build schema dict for LLM consumption
    schema = {
        "database_url_mask": db_url.split("@")[-1] if "@" in db_url else "(set)",
        "ingest_engine_tables_sought": list(TABLES),
        "tables": {}
    }
    if not rows:
        schema["note"] = "No columns found in public schema for ingest tables; public schema may be empty or tables may live in another schema."
    for table_name, column_name, data_type, is_nullable, ordinal_position in rows:
        if table_name not in schema["tables"]:
            schema["tables"][table_name] = {"columns": []}
        schema["tables"][table_name]["columns"].append({
            "name": column_name,
            "data_type": data_type,
            "nullable": is_nullable == "YES",
            "ordinal_position": ordinal_position,
        })

    out_path = sys.argv[1] if len(sys.argv) > 1 else "db_schema.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)
    print(f"Schema written to {out_path}")


if __name__ == '__main__':
    main()
