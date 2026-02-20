"""
Tests for postgres_ingest_engine (source resolution logic; no live DB required).
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import psycopg2
except ImportError:
    psycopg2 = None

from ingestion.schema_loader import Schema
from ingestion.postgres_ingest_engine import PostgresIngestEngine


def test_ensure_source_id_insert_new():
    """First insert returns new id."""
    if not psycopg2:
        print("Skip (psycopg2 not installed)")
        return
    schema_path = Path(__file__).parent.parent / 'schemas' / 'monet' / 'schema.json'
    schema = Schema(str(schema_path))
    logger = MagicMock()
    engine = PostgresIngestEngine("postgresql://localhost/db", schema, logger)
    cursor = MagicMock()
    cursor.fetchone.return_value = (42,)
    cursor.lastrowid = None
    id_ = engine._ensure_source_id(cursor, "New Source")
    assert id_ == 42
    cursor.execute.assert_called_once()
    assert "INSERT INTO sources" in cursor.execute.call_args[0][0]
    assert "RETURNING id" in cursor.execute.call_args[0][0]


def test_ensure_source_id_unique_violation_fallback():
    """On IntegrityError, fallback to SELECT by upper(name)."""
    if not psycopg2:
        print("Skip (psycopg2 not installed)")
        return
    schema_path = Path(__file__).parent.parent / 'schemas' / 'monet' / 'schema.json'
    schema = Schema(str(schema_path))
    logger = MagicMock()
    engine = PostgresIngestEngine("postgresql://localhost/db", schema, logger)
    cursor = MagicMock()
    cursor.execute.side_effect = [
        psycopg2.IntegrityError(),
        None,
    ]
    cursor.fetchone.return_value = (99,)
    id_ = engine._ensure_source_id(cursor, "Existing Source")
    assert id_ == 99
    assert cursor.execute.call_count == 2
    second_call = cursor.execute.call_args_list[1][0][0]
    assert "upper(name)" in second_call and "upper(%s)" in second_call


def test_bulk_insert_leads_empty():
    """Empty leads_df returns empty list."""
    if not psycopg2:
        print("Skip (psycopg2 not installed)")
        return
    schema_path = Path(__file__).parent.parent / 'schemas' / 'monet' / 'schema.json'
    schema = Schema(str(schema_path))
    logger = MagicMock()
    engine = PostgresIngestEngine("postgresql://localhost/db", schema, logger)
    cursor = MagicMock()
    ids = engine._bulk_insert_leads_deterministic(cursor, pd.DataFrame(), ["business_legal_name"])
    assert ids == []


if __name__ == '__main__':
    if not psycopg2:
        print("Skipping postgres_ingest_engine tests (psycopg2 not installed)")
        sys.exit(0)
    test_ensure_source_id_insert_new()
    print("OK test_ensure_source_id_insert_new")
    test_ensure_source_id_unique_violation_fallback()
    print("OK test_ensure_source_id_unique_violation_fallback")
    test_bulk_insert_leads_empty()
    print("OK test_bulk_insert_leads_empty")
    print("All postgres_ingest_engine tests passed.")
