"""Quick check: does the DB have the four tables and any data?"""
import os
import sys
try:
    import psycopg2
except ImportError:
    print("psycopg2 not installed")
    sys.exit(1)

db_url = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/leadpool_migtest")
print("Connecting to:", db_url.split("@")[-1] if "@" in db_url else db_url)

try:
    conn = psycopg2.connect(db_url)
except Exception as e:
    print("Connection failed:", e)
    sys.exit(1)

cur = conn.cursor()

# Tables in public
cur.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public' ORDER BY table_name
""")
tables = [r[0] for r in cur.fetchall()]
print("Tables in public schema:", tables if tables else "(none)")

for t in tables:
    try:
        cur.execute("SELECT count(*) FROM public." + t)
        n = cur.fetchone()[0]
        print("  %s: %d rows" % (t, n))
    except Exception as e:
        print("  %s: error - %s" % (t, e))

cur.close()
conn.close()
print("Done.")
