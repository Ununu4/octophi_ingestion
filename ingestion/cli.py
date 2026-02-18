"""
CLI Module

Command-line interface for the OCTOPHI Ingestion System.
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.schema_loader import Schema
from ingestion.header_mapper import HeaderMapper
from ingestion.template_mapper import TemplateMapper
from ingestion.type_normalizer import Normalizer
from ingestion.deep_cleaner import DeepCleaner
from ingestion.ingest_engine import IngestEngine
from utils.logger import setup_logger
from utils.file_ops import validate_file_path, get_file_size_mb


def select_database():
    """
    Prompt user to select target database.
    
    Returns:
        str: Selected database path
    """
    db1_path = r'C:\Users\ottog\Desktop\monet_database_engine\unified_database_migrated.sqlite'
    db2_path = r'C:\Users\ottog\Desktop\monet_database_engine\unified_database_migrated_2.sqlite'
    
    print()
    print("=" * 70)
    print("DATABASE SELECTION")
    print("=" * 70)
    print()
    print("Please select target database:")
    print()
    print("  [1] Current Database (db1)")
    print(f"      {db1_path}")
    print()
    print("  [2] New Database (db2)")
    print(f"      {db2_path}")
    print()
    
    while True:
        choice = input("Enter your choice (1 or 2): ").strip()
        
        if choice == '1':
            print()
            print(f"✓ Selected: Current Database (db1)")
            print(f"  Path: {db1_path}")
            print()
            return db1_path
        elif choice == '2':
            print()
            print(f"✓ Selected: New Database (db2)")
            print(f"  Path: {db2_path}")
            print()
            return db2_path
        else:
            print("❌ Invalid choice. Please enter 1 or 2.")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='🐙 OCTOPHI Ingestion System V1 - Schema-Driven ETL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic ingestion
  python -m ingestion.cli --schema monet --file data.xlsx --db monet.sqlite --source "Broker List"
  
  # With custom upload tag
  python -m ingestion.cli --schema monet --file data.csv --db monet.sqlite \\
      --upload-tag "2025_12_08_batch_1" --source "Partner Feed"
  
  # With index creation
  python -m ingestion.cli --schema monet --file data.xlsx --db monet.sqlite \\
      --source "New Source" --create-indexes
        """
    )
    
    # Required arguments
    parser.add_argument(
        '--schema',
        required=True,
        help='Schema name (e.g., "monet")'
    )
    parser.add_argument(
        '--file',
        required=True,
        help='Input file path (CSV or XLSX)'
    )
    parser.add_argument(
        '--db',
        required=False,
        default=None,
        help='Database file path (SQLite) - if not provided, will prompt for selection'
    )
    parser.add_argument(
        '--source',
        required=True,
        dest='source_name',
        help='Source name for this data'
    )
    
    # Optional arguments
    parser.add_argument(
        '--upload-tag',
        dest='upload_tag',
        default=None,
        help='Upload tag (default: auto-generated timestamp)'
    )
    parser.add_argument(
        '--create-indexes',
        action='store_true',
        help='Create performance indexes after ingestion'
    )
    parser.add_argument(
        '--log-file',
        dest='log_file',
        default=None,
        help='Log file path (default: console only)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate and preview only, do not insert data'
    )
    parser.add_argument(
        '--skip-appendix',
        action='store_true',
        help='Skip appendix data insertion (only insert mapped fields)'
    )
    parser.add_argument(
        '--mapping-template',
        dest='mapping_template',
        default=None,
        help='Path to CSV template for preprocessing header mapping (bypasses fuzzy matching)'
    )
    
    args = parser.parse_args()
    
    # Setup logger
    logger = setup_logger(log_file=args.log_file)
    
    # Print header
    print("=" * 70)
    print("OCTOPHI INGESTION SYSTEM V1")
    print("=" * 70)
    print()
    
    try:
        # Select database if not provided via command line
        if args.db is None:
            db_path = select_database()
        else:
            db_path = args.db
        
        # Generate upload tag if not provided
        upload_tag = args.upload_tag or datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Validate input file
        logger.info(f"Validating input file: {args.file}")
        file_path = validate_file_path(args.file, ['.csv', '.xlsx', '.xls'])
        file_size = get_file_size_mb(str(file_path))
        logger.info(f"[OK] File validated: {file_size:.2f} MB")
        
        # Build schema paths
        schema_dir = Path(__file__).parent.parent / 'schemas' / args.schema
        schema_path = schema_dir / 'schema.json'
        
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema not found: {schema_path}")
        
        # Load schema
        logger.info(f"Loading schema: {args.schema}")
        schema = Schema(str(schema_path))
        logger.info(f"[OK] Schema loaded: {schema.get_schema_name()} v{schema.get_version()}")
        
        # Load header mapper (template-based or fuzzy-based)
        print()
        if args.mapping_template:
            # ============================================================
            # PREPROCESSING MODE: Direct Template Mapping
            # - No fuzzy logic overhead
            # - Lightning-fast direct lookup
            # - 100% accuracy from explicit mappings
            # ============================================================
            print("=" * 70)
            print("PREPROCESSING MODE: Direct Template Mapping")
            print("=" * 70)
            logger.info(f"Loading template: {args.mapping_template}")
            mapper = TemplateMapper(args.mapping_template)
            template_mappings = mapper.get_mapping_summary()
            logger.info(f"[OK] Loaded {len(template_mappings)} explicit mappings")
            print(f"  [FAST] No fuzzy overhead - direct dictionary lookup")
            print(f"  [ACCURATE] Explicit mappings guarantee correct fields")
        else:
            # ============================================================
            # FUZZY MODE: Intelligent Pattern Matching
            # - Automatic header recognition
            # - Handles variations and typos
            # - Field combinations and computations
            # ============================================================
            print("=" * 70)
            print("FUZZY MODE: Intelligent Pattern Matching")
            print("=" * 70)
            fuzzy_path = schema_dir / 'fuzzy.json'
            if not fuzzy_path.exists():
                raise FileNotFoundError(f"Fuzzy map not found: {fuzzy_path}")
            
            logger.info(f"Loading fuzzy mappings...")
            mapper = HeaderMapper(str(fuzzy_path))
            logger.info(f"[OK] Fuzzy mappings loaded")
            print(f"  [SMART] Automatic pattern recognition")
            print(f"  [FLEXIBLE] Handles header variations")
        
        # Initialize normalizer
        normalizer = Normalizer()
        
        # Initialize deep cleaner
        logger.info(f"Initializing deep cleaner...")
        cleaner = DeepCleaner(schema, mapper, normalizer)
        
        # Clean file
        print()
        logger.info(f"[CLEAN] Processing file: {file_path.name}")
        print("-" * 70)
        leads_df, owners_df, appendix_df = cleaner.clean_file(str(file_path), upload_tag)
        print("-" * 70)
        
        # Skip appendix if requested
        if args.skip_appendix:
            import pandas as pd
            appendix_df = pd.DataFrame()  # Empty DataFrame
            logger.info("[SKIP] Appendix data will not be inserted")
        
        # Validate required fields
        errors = cleaner.validate_required_fields(leads_df, owners_df)
        if errors:
            logger.error("[ERROR] Validation failed:")
            for error in errors:
                logger.error(f"   - {error}")
            sys.exit(1)
        
        logger.info("[OK] Validation passed")
        
        # Print summary
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
                    for key, val in lead.items():
                        if val is not None and val != '':
                            print(f"  {key}: {val}")
            if len(owners_df) > 0:
                print("\nPreview of first 5 owners:")
                for i in range(min(5, len(owners_df))):
                    print(f"\n--- Owner {i+1} ---")
                    owner = owners_df.iloc[i].to_dict()
                    for key, val in owner.items():
                        if val is not None and val != '':
                            print(f"  {key}: {val}")
            sys.exit(0)
        
        # Initialize ingest engine
        logger.info(f"Connecting to database: {db_path}")
        engine = IngestEngine(db_path, schema)
        
        # Ingest data
        engine.ingest(leads_df, owners_df, appendix_df, upload_tag, args.source_name)
        
        # Create indexes if requested
        if args.create_indexes:
            engine.create_indexes()
        
        # Final summary
        print()
        print("=" * 70)
        print("INGESTION COMPLETE")
        print("=" * 70)
        print(f"Source:      {args.source_name}")
        print(f"Upload tag:  {upload_tag}")
        print(f"Database:    {db_path}")
        print(f"Leads:       {len(leads_df):,}")
        print(f"Owners:      {len(owners_df):,}")
        print(f"Appendix:    {len(appendix_df):,}")
        print("=" * 70)
        print()
        
    except FileNotFoundError as e:
        logger.error(f"[ERROR] File not found: {e}")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"[ERROR] Validation error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()




