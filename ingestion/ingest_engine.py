"""
Ingest Engine Module

Handles database operations for ingesting cleaned data.
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional
from datetime import datetime
from .schema_loader import Schema


class IngestEngine:
    """Manages database ingestion operations."""
    
    def __init__(self, db_path: str, schema: Schema):
        """
        Initialize ingest engine.
        
        Args:
            db_path: Path to SQLite database
            schema: Schema loader instance
        """
        self.db_path = Path(db_path)
        self.schema = schema
        
        if not self.db_path.exists():
            print(f"⚠️  Database file not found at {db_path}")
            print(f"    Creating new database...")
        
    def ingest(self, leads_df: pd.DataFrame, owners_df: pd.DataFrame, 
               appendix_df: pd.DataFrame, upload_tag: str, source_name: str):
        """
        Ingest cleaned data into database.
        
        Args:
            leads_df: Cleaned leads DataFrame
            owners_df: Cleaned owners DataFrame
            appendix_df: Appendix DataFrame
            upload_tag: Tag for this upload batch
            source_name: Name of the data source
        """
        print(f"\n[START] Starting ingestion...")
        print(f"   Source: {source_name}")
        print(f"   Upload tag: {upload_tag}")
        print(f"   Records: {len(leads_df)} leads, {len(owners_df)} owners, {len(appendix_df)} appendix rows")
        
        with sqlite3.connect(self.db_path) as conn:
            # Ensure tables exist
            self._ensure_tables(conn)
            
            # Insert or get source
            source_id = self._ensure_source(conn, source_name)
            print(f"[OK] Source ID: {source_id}")
            
            # Add source_id to leads
            leads_df = leads_df.copy()
            leads_df['source_id'] = source_id
            
            # Insert leads
            lead_ids = self._insert_leads(conn, leads_df)
            print(f"[OK] Inserted {len(lead_ids)} leads")
            
            # Insert owners (one per lead)
            self._insert_owners(conn, owners_df, lead_ids)
            print(f"[OK] Inserted {len(lead_ids)} owners")
            
            # Insert appendix rows
            if len(appendix_df) > 0:
                self._insert_appendix(conn, appendix_df, lead_ids, source_id, upload_tag)
                print(f"[OK] Inserted {len(appendix_df)} appendix rows")
            
            conn.commit()
            print(f"[OK] Ingestion complete!")
    
    def _ensure_tables(self, conn: sqlite3.Connection):
        """Ensure all required tables exist."""
        cursor = conn.cursor()
        
        # Create sources table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                notes TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        
        # Create leads table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_legal_name TEXT,
                dba TEXT,
                business_address TEXT,
                business_city TEXT,
                business_state TEXT,
                zip TEXT,
                phone_raw TEXT,
                phone_clean TEXT,
                start_date TEXT,
                tax_id TEXT,
                soc TEXT,
                business_email TEXT,
                business_second_email TEXT,
                second_phone_raw TEXT,
                second_phone_clean TEXT,
                source_id INTEGER,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT,
                FOREIGN KEY (source_id) REFERENCES sources(id)
            )
        """)
        
        # Create owners table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS owners (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL,
                owner_name TEXT,
                dob TEXT,
                ssn TEXT,
                owner_address TEXT,
                owner_city TEXT,
                owner_state TEXT,
                owner_zip TEXT,
                owner_email TEXT,
                owner_second_email TEXT,
                owner_phone TEXT,
                owner_phone_clean TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT,
                FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE,
                UNIQUE (lead_id)
            )
        """)
        
        # Create appendix table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS lead_appendix_kv (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL,
                source_id INTEGER,
                upload_tag TEXT,
                original_row_number INTEGER,
                column_name TEXT NOT NULL,
                value TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE,
                FOREIGN KEY (source_id) REFERENCES sources(id)
            )
        """)
        
        conn.commit()
    
    def _ensure_source(self, conn: sqlite3.Connection, source_name: str) -> int:
        """
        Ensure source exists in database.
        
        Args:
            conn: Database connection
            source_name: Name of source
            
        Returns:
            Source ID
        """
        cursor = conn.cursor()
        
        # Try to insert (will fail if exists due to UNIQUE constraint)
        try:
            cursor.execute(
                "INSERT INTO sources (name) VALUES (?)",
                (source_name,)
            )
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            # Source already exists, get its ID
            cursor.execute(
                "SELECT id FROM sources WHERE name = ?",
                (source_name,)
            )
            result = cursor.fetchone()
            return result[0]
    
    def _insert_leads(self, conn: sqlite3.Connection, leads_df: pd.DataFrame) -> list:
        """
        Insert leads and return their IDs.
        
        Args:
            conn: Database connection
            leads_df: Leads DataFrame
            
        Returns:
            List of lead IDs
        """
        cursor = conn.cursor()
        lead_ids = []
        
        # Get lead fields from schema (excluding system-generated)
        lead_fields = [f for f in self.schema.fields('lead') 
                       if not self.schema.is_system_generated('lead', f)]
        
        # Add source_id if present
        if 'source_id' in leads_df.columns and 'source_id' not in lead_fields:
            lead_fields.append('source_id')
        
        # Build insert statement
        columns = ', '.join(lead_fields)
        placeholders = ', '.join(['?' for _ in lead_fields])
        sql = f"INSERT INTO leads ({columns}) VALUES ({placeholders})"
        
        # Insert each row
        for _, row in leads_df.iterrows():
            values = [row.get(f) for f in lead_fields]
            cursor.execute(sql, values)
            lead_ids.append(cursor.lastrowid)
        
        return lead_ids
    
    def _insert_owners(self, conn: sqlite3.Connection, owners_df: pd.DataFrame, lead_ids: list):
        """
        Insert owners (one per lead).
        
        Args:
            conn: Database connection
            owners_df: Owners DataFrame
            lead_ids: List of lead IDs (from _insert_leads)
        """
        cursor = conn.cursor()
        
        # Get owner fields from schema (excluding system-generated)
        owner_fields = [f for f in self.schema.fields('owner') 
                        if not self.schema.is_system_generated('owner', f)]
        
        # Build insert statement
        columns = 'lead_id, ' + ', '.join(owner_fields)
        placeholders = ', '.join(['?' for _ in owner_fields] + ['?'])
        sql = f"INSERT INTO owners ({columns}) VALUES ({placeholders})"
        
        # Insert each row
        for idx, row in owners_df.iterrows():
            lead_id = lead_ids[idx]
            values = [row.get(f) for f in owner_fields] + [lead_id]
            # Reorder: lead_id first
            values = [lead_id] + [row.get(f) for f in owner_fields]
            cursor.execute(sql, values)
    
    def _insert_appendix(self, conn: sqlite3.Connection, appendix_df: pd.DataFrame, 
                         lead_ids: list, source_id: int, upload_tag: str):
        """
        Insert appendix rows.
        
        Args:
            conn: Database connection
            appendix_df: Appendix DataFrame
            lead_ids: List of lead IDs
            source_id: Source ID
            upload_tag: Upload tag
        """
        cursor = conn.cursor()
        
        # Replace lead_id_placeholder with actual lead_id
        rows = []
        for _, row in appendix_df.iterrows():
            placeholder_idx = int(row['lead_id_placeholder'])
            actual_lead_id = lead_ids[placeholder_idx]
            
            rows.append((
                actual_lead_id,
                source_id,
                upload_tag,
                row['original_row_number'],
                row['column_name'],
                row['value']
            ))
        
        # Bulk insert
        cursor.executemany(
            """
            INSERT INTO lead_appendix_kv 
            (lead_id, source_id, upload_tag, original_row_number, column_name, value)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows
        )
    
    def create_indexes(self):
        """Create performance indexes on tables."""
        print("\n📊 Creating indexes...")
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_leads_state ON leads(business_state)",
                "CREATE INDEX IF NOT EXISTS idx_leads_zip ON leads(zip)",
                "CREATE INDEX IF NOT EXISTS idx_leads_source ON leads(source_id)",
                "CREATE INDEX IF NOT EXISTS idx_leads_phone_clean ON leads(phone_clean)",
                "CREATE INDEX IF NOT EXISTS idx_leads_business_email ON leads(business_email)",
                "CREATE INDEX IF NOT EXISTS idx_owners_lead ON owners(lead_id)",
                "CREATE INDEX IF NOT EXISTS idx_owners_owner_phone_clean ON owners(owner_phone_clean)",
                "CREATE INDEX IF NOT EXISTS idx_lead_appendix_lead ON lead_appendix_kv(lead_id)",
                "CREATE INDEX IF NOT EXISTS idx_lead_appendix_col ON lead_appendix_kv(column_name)",
                "CREATE INDEX IF NOT EXISTS idx_lead_appendix_upload ON lead_appendix_kv(upload_tag)",
            ]
            
            for idx_sql in indexes:
                try:
                    cursor.execute(idx_sql)
                except sqlite3.Error as e:
                    print(f"⚠️  Index creation warning: {e}")
            
            conn.commit()
            print(f"[OK] Created {len(indexes)} indexes")




