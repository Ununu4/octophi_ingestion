"""
CLI Module

Command-line interface for the OCTOPHI Ingestion System (Postgres, template-only).
"""

import argparse
import os
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from config import DEFAULT_DATABASE_URL
except ImportError:
    DEFAULT_DATABASE_URL = None

from ingestion.schema_loader import Schema
from ingestion.template_mapper import TemplateMapper
from ingestion.type_normalizer import Normalizer
from ingestion.deep_cleaner import DeepCleaner
from ingestion.postgres_ingest_engine import PostgresIngestEngine
from ingestion.template_validator import validate_template
from utils.logger import setup_logger
from utils.file_ops import validate_file_path, get_file_size_mb


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='OCTOPHI Ingestion System — Postgres, template-only ETL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  export DATABASE_URL="postgresql://user:pass@localhost:5433/mydb"
  python -m ingestion.cli --schema monet --file data.xlsx --source "Vendor" --mapping-template templates/vendor.csv

  python -m ingestion.cli --schema monet --file data.csv --source "Vendor" --mapping-template templates/vendor.csv --dry-run
        """,
    )

    parser.add_argument('--schema', required=True, help='Schema name (e.g., monet)')
    parser.add_argument('--file', required=True, help='Input file path (CSV or XLSX/XLS)')
    parser.add_argument('--source', required=True, dest='source_name', help='Source name for this data')
    parser.add_argument('--mapping-template', required=True, dest='mapping_template',
                        help='Path to CSV template: incoming_schema,expected_schema (required)')
    parser.add_argument('--db-url', dest='db_url', default=None,
                        help='PostgreSQL URL (default: DATABASE_URL env var)')
    parser.add_argument('--upload-tag', dest='upload_tag', default=None,
                        help='Upload tag (default: timestamp)')
    parser.add_argument('--log-file', dest='log_file', default=None, help='Log file path')
    parser.add_argument('--dry-run', action='store_true',
                        help='Validate and preview only; do not insert')
    parser.add_argument('--skip-appendix', action='store_true',
                        help='Do not insert appendix rows')

    args = parser.parse_args()

    logger = setup_logger(log_file=args.log_file)

    print("=" * 70)
    print("OCTOPHI INGESTION (Postgres, template-only)")
    print("=" * 70)
    print()

    try:
        db_url = args.db_url or os.environ.get('DATABASE_URL') or DEFAULT_DATABASE_URL
        if not db_url and not args.dry_run:
            logger.error("[ERROR] Database URL required. Set DATABASE_URL, use --db-url, or ensure config.DEFAULT_DATABASE_URL")
            sys.exit(1)
        if db_url:
            logger.info("Database: (from DATABASE_URL / --db-url)")

        upload_tag = args.upload_tag or datetime.now().strftime('%Y%m%d_%H%M%S')

        logger.info(f"Validating input file: {args.file}")
        file_path = validate_file_path(args.file, ['.csv', '.xlsx', '.xls'])
        file_size = get_file_size_mb(str(file_path))
        logger.info(f"[OK] File validated: {file_size:.2f} MB")

        schema_dir = Path(__file__).parent.parent / 'schemas' / args.schema
        schema_path = schema_dir / 'schema.json'
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema not found: {schema_path}")

        logger.info(f"Loading schema: {args.schema}")
        schema = Schema(str(schema_path))
        logger.info(f"[OK] Schema loaded: {schema.get_schema_name()} v{schema.get_version()}")

        logger.info(f"Loading template: {args.mapping_template}")
        mapper = TemplateMapper(args.mapping_template)
        logger.info(f"[OK] Loaded {len(mapper.get_mapping_summary())} explicit mappings")

        template_errors = validate_template(schema, mapper)
        if template_errors:
            logger.error("[ERROR] Template validation failed:")
            for err in template_errors:
                logger.error(f"   - {err}")
            sys.exit(1)
        logger.info("[OK] Template validation passed")

        normalizer = Normalizer()
        cleaner = DeepCleaner(schema, mapper, normalizer)

        print()
        logger.info(f"[CLEAN] Processing file: {file_path.name}")
        print("-" * 70)
        leads_df, owners_df, appendix_df = cleaner.clean_file(str(file_path), upload_tag)
        print("-" * 70)

        if args.skip_appendix:
            import pandas as pd
            appendix_df = pd.DataFrame()
            logger.info("[SKIP] Appendix data will not be inserted")

        errors = cleaner.validate_required_fields(leads_df, owners_df)
        if errors:
            logger.error("[ERROR] Validation failed:")
            for error in errors:
                logger.error(f"   - {error}")
            sys.exit(1)
        logger.info("[OK] Validation passed")

        print()
        print("CLEANING SUMMARY")
        print("-" * 70)
        print(f"Leads:    {len(leads_df):>8,} rows")
        print(f"Owners:   {len(owners_df):>8,} rows")
        print(f"Appendix: {len(appendix_df):>8,} rows")
        print("-" * 70)

        if args.dry_run:
            print()
            logger.info("[DRY-RUN] No data inserted")
            print()
            print("Preview of first 5 leads:")
            if len(leads_df) > 0:
                for i in range(min(5, len(leads_df))):
                    print(f"\n--- Record {i+1} ---")
                    lead = leads_df.iloc[i].to_dict()
                    for k, v in lead.items():
                        if v is not None and v != '':
                            print(f"  {k}: {v}")
            if len(owners_df) > 0:
                print("\nPreview of first 5 owners:")
                for i in range(min(5, len(owners_df))):
                    print(f"\n--- Owner {i+1} ---")
                    owner = owners_df.iloc[i].to_dict()
                    for k, v in owner.items():
                        if v is not None and v != '':
                            print(f"  {k}: {v}")
            sys.exit(0)

        engine = PostgresIngestEngine(db_url, schema, logger)
        summary = engine.ingest(
            leads_df, owners_df, appendix_df, upload_tag, args.source_name,
            file_name=file_path.name,
            file_size_mb=file_size,
        )

        print()
        print("=" * 70)
        print("INGESTION COMPLETE")
        print("=" * 70)
        print(f"Source:      {summary['source_name']}")
        print(f"Upload tag:  {summary['upload_tag']}")
        print(f"Source ID:   {summary['source_id']}")
        print(f"File:        {summary.get('file_name', '—')} ({summary.get('file_size_mb', 0):.2f} MB)")
        print(f"Leads:       {summary['leads_inserted']:,}")
        print(f"Owners:      {summary['owners_inserted']:,}")
        print(f"Appendix:    {summary['appendix_inserted']:,}")
        print(f"Elapsed:     {summary['elapsed_seconds']:.2f}s")
        if summary.get('timings'):
            for k, v in summary['timings'].items():
                if k != 'total':
                    print(f"  {k}: {v}s")
        print("=" * 70)
        print()

    except FileNotFoundError as e:
        logger.error(f"[ERROR] File not found: {e}")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"[ERROR] Validation error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
