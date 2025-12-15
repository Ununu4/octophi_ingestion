"""Test duration conversion examples."""
import pandas as pd
from datetime import datetime

# Read the CSV
df = pd.read_csv(r'C:\Users\ottog\Desktop\READY_CSV_E\jlo_mm1084.csv', dtype=str)

# Import duration parser
from ingestion.duration_parser import DurationParser

print("=" * 70)
print("DURATION TO DATE CONVERSION TEST")
print("=" * 70)
print(f"Reference date: {datetime.now().strftime('%Y-%m-%d')}")
print()

# Get unique TIB values
tib_samples = df['TIB'].dropna().unique()[:10]

print("Sample Conversions:")
print("-" * 70)
print(f"{'Original TIB Value':<30} | {'Computed Start Date':<20}")
print("-" * 70)

for tib in tib_samples:
    start_date = DurationParser.parse_duration_to_date(tib)
    print(f"{tib:<30} | {start_date or 'N/A':<20}")

print()
print("=" * 70)


