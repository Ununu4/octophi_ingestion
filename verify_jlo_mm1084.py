"""Verify jlo_mm1084 ingestion results."""
import sqlite3

db_path = r'C:\Users\ottog\Desktop\monet_database_engine\unified_database_migrated.sqlite'

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("=" * 80)
print("VERIFICATION: jlo_mm1084 INGESTION")
print("=" * 80)
print()

# Get source info
cursor.execute("SELECT id, name FROM sources WHERE name = 'jlo_mm1084'")
source = cursor.fetchone()
source_id = source[0]

print(f"✅ Source: {source[1]} (ID: {source_id})")
print()

# Sample leads with start_date and owner_name
cursor.execute("""
    SELECT 
        l.id,
        l.business_legal_name,
        l.start_date,
        o.owner_name,
        l.phone_clean
    FROM leads l
    JOIN owners o ON o.lead_id = l.id
    WHERE l.source_id = ?
    ORDER BY l.id DESC
    LIMIT 10
""", (source_id,))

print("Sample Leads (with computed start_date and combined owner_name):")
print("-" * 80)
print(f"{'ID':<9} | {'Business Name':<30} | {'Start Date':<12} | {'Owner Name':<20}")
print("-" * 80)

for row in cursor.fetchall():
    print(f"{row[0]:<9} | {row[1][:30]:<30} | {row[2] or 'N/A':<12} | {row[3] or 'N/A':<20}")

print()

# Check appendix columns
cursor.execute("""
    SELECT DISTINCT column_name
    FROM lead_appendix_kv
    WHERE source_id = ?
""", (source_id,))

appendix_cols = [row[0] for row in cursor.fetchall()]
print(f"Appendix columns stored: {', '.join(appendix_cols)}")
print()

# Sample appendix data
cursor.execute("""
    SELECT l.business_legal_name, a.column_name, a.value
    FROM lead_appendix_kv a
    JOIN leads l ON l.id = a.lead_id
    WHERE a.source_id = ?
    LIMIT 5
""", (source_id,))

print("Sample Appendix Data:")
print("-" * 80)
for row in cursor.fetchall():
    print(f"  {row[0][:40]:<40} | {row[1]:<15} | {row[2][:30]}")

print()

# Count stats
cursor.execute("SELECT COUNT(*) FROM leads WHERE source_id = ?", (source_id,))
lead_count = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM owners o JOIN leads l ON o.lead_id = l.id WHERE l.source_id = ?", (source_id,))
owner_count = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM lead_appendix_kv WHERE source_id = ?", (source_id,))
appendix_count = cursor.fetchone()[0]

print("=" * 80)
print("FINAL COUNTS:")
print(f"  Leads: {lead_count:,}")
print(f"  Owners: {owner_count:,}")
print(f"  Appendix rows: {appendix_count:,}")
print("=" * 80)

conn.close()


