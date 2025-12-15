"""Preview owner data from last dry-run."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from ingestion.schema_loader import Schema
from ingestion.header_mapper import HeaderMapper
from ingestion.type_normalizer import Normalizer
from ingestion.deep_cleaner import DeepCleaner

# Load components
schema = Schema('schemas/monet/schema.json')
mapper = HeaderMapper('schemas/monet/fuzzy.json')
normalizer = Normalizer()
cleaner = DeepCleaner(schema, mapper, normalizer)

# Clean file
input_file = r'C:\Users\ottog\Desktop\READY_CSV_E\jlo_mm1084.csv'
leads_df, owners_df, appendix_df = cleaner.clean_file(input_file, 'preview')

print("\n" + "=" * 70)
print("OWNER DATA PREVIEW")
print("=" * 70)
print(f"\nTotal owners: {len(owners_df)}")
print(f"\nFirst 5 owners:")
print(owners_df[['owner_name']].head(10))
print("\nOwner columns:", owners_df.columns.tolist())


