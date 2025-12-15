"""Quick script to check source linking."""
import sqlite3
import sys

db_path = '../unified_database_migrated.sqlite'

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("=" * 80)
    print("SOURCE VERIFICATION")
    print("=" * 80)
    print()
    
    # Check sources table
    cursor.execute('SELECT id, name, created_at FROM sources ORDER BY id DESC LIMIT 10')
    sources = cursor.fetchall()
    
    print("SOURCES IN DATABASE:")
    print("-" * 80)
    print(f"{'ID':<5} | {'Name':<40} | {'Created At':<20}")
    print("-" * 80)
    for row in sources:
        print(f"{row[0]:<5} | {row[1]:<40} | {row[2]:<20}")
    
    print()
    print("=" * 80)
    print("LEAD COUNT BY SOURCE:")
    print("-" * 80)
    
    # Check lead counts by source
    cursor.execute('''
        SELECT s.id, s.name, COUNT(l.id) as lead_count 
        FROM sources s 
        LEFT JOIN leads l ON l.source_id = s.id 
        GROUP BY s.id 
        ORDER BY s.id DESC 
        LIMIT 10
    ''')
    
    print(f"{'Source ID':<10} | {'Source Name':<40} | {'Lead Count':<12}")
    print("-" * 80)
    for row in cursor.fetchall():
        print(f"{row[0]:<10} | {row[1]:<40} | {row[2]:<12}")
    
    print()
    print("=" * 80)
    print("SAMPLE LEADS FROM Sean_Nov_Dec_NOV:")
    print("-" * 80)
    
    # Check specific source
    cursor.execute('''
        SELECT l.id, l.business_legal_name, l.phone_clean, s.name as source_name
        FROM leads l
        LEFT JOIN sources s ON l.source_id = s.id
        WHERE s.name = 'Sean_Nov_Dec_NOV'
        LIMIT 5
    ''')
    
    results = cursor.fetchall()
    if results:
        print(f"{'Lead ID':<10} | {'Business Name':<40} | {'Phone':<15} | {'Source':<20}")
        print("-" * 80)
        for row in results:
            print(f"{row[0]:<10} | {row[1]:<40} | {row[2] or 'N/A':<15} | {row[3]:<20}")
    else:
        print("❌ No leads found with source 'Sean_Nov_Dec_NOV'")
    
    conn.close()
    print()
    print("=" * 80)
    
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)




