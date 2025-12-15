"""Verify data in the correct production database."""
import sqlite3

db_path = r'C:\Users\ottog\Desktop\monet_database_engine\unified_database_migrated.sqlite'

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("=" * 80)
    print("VERIFICATION: PRODUCTION DATABASE")
    print("=" * 80)
    print()
    
    # Check if Sean_Nov_Dec_NOV source exists
    cursor.execute("SELECT id, name FROM sources WHERE name = 'Sean_Nov_Dec_NOV'")
    source = cursor.fetchone()
    
    if source:
        source_id = source[0]
        print(f"✅ Source found: '{source[1]}' (ID: {source_id})")
        print()
        
        # Count leads for this source
        cursor.execute("SELECT COUNT(*) FROM leads WHERE source_id = ?", (source_id,))
        lead_count = cursor.fetchone()[0]
        print(f"✅ Lead count: {lead_count}")
        
        # Get sample leads
        cursor.execute("""
            SELECT l.id, l.business_legal_name, o.owner_name, l.phone_clean
            FROM leads l
            JOIN owners o ON o.lead_id = l.id
            WHERE l.source_id = ?
            ORDER BY l.id DESC
            LIMIT 5
        """, (source_id,))
        
        print()
        print("Sample leads (most recent):")
        print("-" * 80)
        for row in cursor.fetchall():
            print(f"  ID: {row[0]} | {row[1]} | Owner: {row[2]} | Phone: {row[3]}")
    else:
        print("❌ Source 'Sean_Nov_Dec_NOV' not found!")
    
    print()
    print("=" * 80)
    print("All sources in production database:")
    print("-" * 80)
    cursor.execute("SELECT id, name, created_at FROM sources ORDER BY id DESC LIMIT 10")
    for row in cursor.fetchall():
        print(f"  {row[0]:3} | {row[1]:40} | {row[2]}")
    
    conn.close()
    print()
    print("✅ Verification complete!")
    print("=" * 80)
    
except Exception as e:
    print(f"❌ Error: {e}")




