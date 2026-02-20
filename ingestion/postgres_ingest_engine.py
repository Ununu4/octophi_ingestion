"""
Postgres Ingest Engine (hardened).

Bulk ingestion into PostgreSQL. One transaction; deterministic lead ID order;
batching; source resolution with retry; appendix placeholder validation.
"""

import logging
import time
from typing import Dict, List, Any

import pandas as pd

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    psycopg2 = None
    execute_values = None

from .schema_loader import Schema


LEADS_INSERT_COLUMNS = [
    'business_legal_name', 'dba', 'business_address', 'business_city', 'business_state',
    'zip', 'phone_raw', 'phone_clean', 'start_date', 'tax_id', 'soc',
    'source_id', 'business_email', 'business_second_email', 'second_phone_raw', 'second_phone_clean',
]
OWNERS_INSERT_COLUMNS = [
    'lead_id', 'owner_name', 'dob', 'ssn', 'owner_address', 'owner_city', 'owner_state',
    'owner_zip', 'owner_email', 'owner_second_email', 'owner_phone', 'owner_phone_clean',
]

DEFAULT_PAGE_SIZE = 5000


class PostgresIngestEngine:
    """Ingest cleaned DataFrames into PostgreSQL. Production-hardened."""

    def __init__(
        self,
        db_url: str,
        schema: Schema,
        logger: logging.Logger,
        page_size: int = DEFAULT_PAGE_SIZE,
    ):
        if not psycopg2 or not execute_values:
            raise RuntimeError("psycopg2 is required. Install with: pip install psycopg2-binary")
        self.db_url = db_url
        self.schema = schema
        self.logger = logger
        self.page_size = max(100, page_size)

    def ingest(
        self,
        leads_df: pd.DataFrame,
        owners_df: pd.DataFrame,
        appendix_df: pd.DataFrame,
        upload_tag: str,
        source_name: str,
        file_name: str = None,
        file_size_mb: float = None,
    ) -> Dict[str, Any]:
        """
        Ingest in one transaction. Returns structured summary.
        On failure, rolls back and re-raises.
        """
        t0 = time.perf_counter()
        summary = {
            'source_id': None,
            'leads_inserted': 0,
            'owners_inserted': 0,
            'appendix_inserted': 0,
            'elapsed_seconds': 0.0,
            'source_name': source_name,
            'upload_tag': upload_tag,
            'file_name': file_name,
            'file_size_mb': file_size_mb,
            'total_rows_read': len(leads_df),
            'timings': {},
        }

        conn = psycopg2.connect(self.db_url, connect_timeout=30)
        try:
            conn.autocommit = False
            cursor = conn.cursor()

            self._verify_tables(cursor)

            t1 = time.perf_counter()
            source_id = self._ensure_source_id(cursor, source_name)
            summary['source_id'] = source_id
            summary['timings']['source_resolution'] = round(time.perf_counter() - t1, 3)
            self.logger.info(f"Source ID: {source_id}")

            leads_df = leads_df.copy()
            leads_df['source_id'] = source_id
            insert_cols = [c for c in LEADS_INSERT_COLUMNS if c in leads_df.columns]

            t2 = time.perf_counter()
            lead_ids = self._bulk_insert_leads_deterministic(cursor, leads_df, insert_cols)
            summary['leads_inserted'] = len(lead_ids)
            summary['timings']['leads_insert'] = round(time.perf_counter() - t2, 3)
            self.logger.info(f"Inserted {len(lead_ids)} leads")

            owner_cols = [c for c in OWNERS_INSERT_COLUMNS if c != 'lead_id']
            t3 = time.perf_counter()
            owners_inserted = self._bulk_insert_owners_batched(cursor, owners_df, lead_ids, owner_cols)
            summary['owners_inserted'] = owners_inserted
            summary['timings']['owners_insert'] = round(time.perf_counter() - t3, 3)
            self.logger.info(f"Inserted {owners_inserted} owners")

            if len(appendix_df) > 0:
                t4 = time.perf_counter()
                appendix_inserted = self._bulk_insert_appendix_batched(
                    cursor, appendix_df, lead_ids, source_id, upload_tag
                )
                summary['appendix_inserted'] = appendix_inserted
                summary['timings']['appendix_insert'] = round(time.perf_counter() - t4, 3)
                self.logger.info(f"Inserted {appendix_inserted} appendix rows")

            conn.commit()
            summary['elapsed_seconds'] = round(time.perf_counter() - t0, 3)
            summary['timings']['total'] = summary['elapsed_seconds']
            return summary

        except Exception as e:
            conn.rollback()
            self.logger.error(f"Ingestion failed: {e}")
            raise
        finally:
            conn.close()

    def _verify_tables(self, cursor) -> None:
        """Preflight: required tables must exist. Read-only. Do not create."""
        cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name IN (%s, %s, %s, %s)
        """, ('sources', 'leads', 'owners', 'lead_appendix_kv'))
        found = {row[0] for row in cursor.fetchall()}
        missing = {'sources', 'leads', 'owners', 'lead_appendix_kv'} - found
        if missing:
            raise RuntimeError(
                f"Required tables missing: {sorted(missing)}. Database schema must exist."
            )

    def _ensure_source_id(self, cursor, name: str) -> int:
        """Insert or get source by name. Uniqueness on upper(name). Retry once if SELECT returns nothing."""
        try:
            cursor.execute(
                "INSERT INTO sources (name) VALUES (%s) RETURNING id",
                (name,),
            )
            row = cursor.fetchone()
            return row[0]
        except psycopg2.IntegrityError:
            cursor.execute(
                "SELECT id FROM sources WHERE upper(name) = upper(%s)",
                (name,),
            )
            row = cursor.fetchone()
            if row is not None:
                return row[0]
            try:
                cursor.execute(
                    "INSERT INTO sources (name) VALUES (%s) RETURNING id",
                    (name,),
                )
                row = cursor.fetchone()
                if row is not None:
                    return row[0]
            except psycopg2.IntegrityError:
                pass
            raise RuntimeError(
                f"Source name conflict for '{name}' but could not resolve or retry insert. "
                "Check unique index on upper(name)."
            )

    def _val(self, v):
        """Convert to psycopg2-compatible Python native type."""
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        if hasattr(v, 'item'):  # numpy int/float
            return v.item()
        return v

    def _bulk_insert_leads_deterministic(
        self, cursor, leads_df: pd.DataFrame, columns: List[str]
    ) -> List[int]:
        """
        Insert leads in batches. Use CTE with row number so RETURNING id order matches input order.
        Each batch: WITH data (cols..., rn) AS (VALUES ...), ins AS (INSERT ... SELECT ... ORDER BY rn RETURNING id) SELECT id FROM ins.
        """
        if len(leads_df) == 0:
            return []

        n_cols = len(columns)
        col_list = ", ".join(columns)
        # rn column for ordering only (not stored). execute_values expands single %s to (row1),(row2),...
        data_cols = col_list + ", rn"
        sql = (
            "WITH data (" + data_cols + ") AS ("
            "SELECT * FROM (VALUES %s) AS v(" + data_cols + ")), "
            "ins AS ("
            "INSERT INTO leads (" + col_list + ") "
            "SELECT " + col_list + " FROM data ORDER BY rn RETURNING id"
            ") SELECT id FROM ins"
        )
        template = "(" + ", ".join(["%s"] * (n_cols + 1)) + ")"
        lead_ids = []
        for start in range(0, len(leads_df), self.page_size):
            end = min(start + self.page_size, len(leads_df))
            batch_df = leads_df.iloc[start:end]
            rows = [
                tuple([self._val(batch_df.loc[batch_df.index[i], c]) for c in columns] + [i])
                for i in range(len(batch_df))
            ]
            execute_values(cursor, sql, rows, template=template, page_size=len(rows))
            ids = cursor.fetchall()
            if ids is None:
                raise RuntimeError("Lead INSERT RETURNING returned no cursor result.")
            lead_ids.extend([r[0] for r in ids])
        return lead_ids

    def _bulk_insert_owners_batched(
        self,
        cursor,
        owners_df: pd.DataFrame,
        lead_ids: List[int],
        owner_columns: List[str],
    ) -> int:
        """Bulk insert owners in batches. owner row i gets lead_id = lead_ids[i]. Raises if length mismatch."""
        if len(owners_df) == 0:
            if len(lead_ids) > 0:
                self.logger.warning("No owner rows but leads exist; inserting leads only.")
            return 0
        if len(owners_df) != len(lead_ids):
            raise ValueError(
                f"owners_df length ({len(owners_df)}) must match lead_ids length ({len(lead_ids)}). "
                "Owners must align row-for-row with leads."
            )
        cols = ['lead_id'] + [c for c in owner_columns if c in owners_df.columns]
        sql = "INSERT INTO owners (" + ", ".join(cols) + ") VALUES %s"
        template = "(" + ", ".join(["%s"] * len(cols)) + ")"
        total = 0
        for start in range(0, len(owners_df), self.page_size):
            end = min(start + self.page_size, len(owners_df))
            batch_rows = []
            for idx in range(start, end):
                row = owners_df.iloc[idx]
                lead_id = lead_ids[idx]
                batch_rows.append(
                    tuple([self._val(lead_id)] + [self._val(row.get(c)) for c in owner_columns if c in owners_df.columns])
                )
            execute_values(cursor, sql, batch_rows, template=template, page_size=len(batch_rows))
            total += len(batch_rows)
        return total

    def _bulk_insert_appendix_batched(
        self,
        cursor,
        appendix_df: pd.DataFrame,
        lead_ids: List[int],
        source_id: int,
        upload_tag: str,
    ) -> int:
        """Bulk insert appendix in batches. Validates placeholder index before resolving."""
        if len(appendix_df) == 0:
            return 0
        n_leads = len(lead_ids)
        rows = []
        for _, row in appendix_df.iterrows():
            placeholder_idx = int(row['lead_id_placeholder'])
            if placeholder_idx < 0 or placeholder_idx >= n_leads:
                raise ValueError(
                    f"Appendix lead_id_placeholder {placeholder_idx} out of range [0, {n_leads - 1}]. "
                    "Check cleaning pipeline."
                )
            actual_lead_id = lead_ids[placeholder_idx]
            rows.append((
                self._val(actual_lead_id),
                self._val(source_id),
                upload_tag,
                self._val(row['original_row_number']),
                self._val(row['column_name']),
                self._val(row['value']),
            ))
        sql = (
            "INSERT INTO lead_appendix_kv "
            "(lead_id, source_id, upload_tag, original_row_number, column_name, value) VALUES %s"
        )
        template = "(%s, %s, %s, %s, %s, %s)"
        total = 0
        for start in range(0, len(rows), self.page_size):
            batch = rows[start:start + self.page_size]
            execute_values(cursor, sql, batch, template=template, page_size=len(batch))
            total += len(batch)
        return total
