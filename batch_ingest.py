"""
Batch Ingestion Module

Bulk ingest multiple files using a single template.
"""

import sys
import argparse
from pathlib import Path
from glob import glob
import subprocess

def main():
    parser = argparse.ArgumentParser(
        description='🐙 OCTOPHI Batch Ingestion - Bulk ingest multiple files with one template',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest all CSV files in a directory
  python batch_ingest.py --template schema.csv --files "C:\\folder\\*.csv"
  
  # Ingest specific files
  python batch_ingest.py --template schema.csv --files "file1.csv" "file2.csv" "file3.csv"
        """
    )
    
    parser.add_argument(
        '--template',
        required=True,
        help='Path to schema mapping template (single template for all files)'
    )
    parser.add_argument(
        '--files',
        required=True,
        nargs='+',
        help='File paths or pattern (e.g., "*.csv" or list of files)'
    )
    parser.add_argument(
        '--db',
        required=False,
        default=None,
        help='Database path (if not provided, will prompt)'
    )
    parser.add_argument(
        '--schema',
        default='monet',
        help='Schema name (default: monet)'
    )
    parser.add_argument(
        '--python',
        default=r'C:\Users\ottog\Desktop\monet_database_engine\venv\Scripts\python.exe',
        help='Python executable path (default: venv python)'
    )
    parser.add_argument(
        '--yes',
        action='store_true',
        help='Auto-confirm ingestion (skip confirmation prompt)'
    )
    parser.add_argument(
        '--dry-run-only',
        action='store_true',
        help='Only run dry run, do not proceed with ingestion'
    )
    parser.add_argument(
        '--source-prefix',
        default=None,
        help='Custom source name prefix (e.g., "CJ" will name files as CJ1, CJ2, etc.)'
    )
    parser.add_argument(
        '--start-index',
        type=int,
        default=1,
        help='Starting index for source naming (default: 1)'
    )
    
    args = parser.parse_args()
    
    # Resolve file patterns
    file_list = []
    for pattern in args.files:
        if '*' in pattern or '?' in pattern:
            # Glob pattern
            matched = glob(pattern)
            file_list.extend(matched)
        else:
            # Direct file path
            file_list.append(pattern)
    
    if not file_list:
        print("[ERROR] No files found matching the pattern!")
        sys.exit(1)
    
    print("=" * 70)
    print("OCTOPHI BATCH INGESTION")
    print("=" * 70)
    print()
    print(f"Template: {args.template}")
    print(f"Files to process: {len(file_list)}")
    print()
    
    # Show files
    for i, file in enumerate(file_list, 1):
        file_name = Path(file).name
        print(f"  {i}. {file_name}")
    print()
    
    # Database selection
    if args.db is None:
        print("Select database:")
        print("  [1] Current Database (db1)")
        print("      C:\\Users\\ottog\\Desktop\\monet_database_engine\\unified_database_migrated.sqlite")
        print("  [2] New Database (db2)")
        print("      C:\\Users\\ottog\\Desktop\\monet_database_engine\\unified_database_migrated_2.sqlite")
        print()
        
        while True:
            choice = input("Enter your choice (1 or 2): ").strip()
            if choice == '1':
                db_path = r'C:\Users\ottog\Desktop\monet_database_engine\unified_database_migrated.sqlite'
                break
            elif choice == '2':
                db_path = r'C:\Users\ottog\Desktop\monet_database_engine\unified_database_migrated_2.sqlite'
                break
            else:
                print("[ERROR] Invalid choice. Please enter 1 or 2.")
    else:
        db_path = args.db
    
    print()
    print(f"Using database: {db_path}")
    print()
    
    # Phase 1: DRY RUN on all files
    print("=" * 70)
    print("PHASE 1: DRY RUN (Validation)")
    print("=" * 70)
    print()
    
    dry_run_results = []
    
    for i, file in enumerate(file_list, args.start_index):
        file_path = Path(file)
        
        # Determine source name
        if args.source_prefix:
            source_name = f"{args.source_prefix}{i}"
        else:
            source_name = file_path.stem  # File name without extension
        
        print(f"[{i}/{len(file_list)}] Dry run: {file_path.name}...", end=' ')
        
        cmd = [
            args.python,
            '-m', 'ingestion.cli',
            '--schema', args.schema,
            '--file', str(file_path.absolute()),
            '--source', source_name,
            '--mapping-template', args.template,
            '--db', db_path,
            '--dry-run'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            # Parse output for row count
            output = result.stdout
            leads_count = 0
            for line in output.split('\n'):
                if 'Leads:' in line and 'rows' in line:
                    try:
                        leads_count = int(line.split()[1].replace(',', ''))
                    except:
                        pass
            
            print(f"OK ({leads_count:,} leads)")
            dry_run_results.append({
                'file': file_path.name,
                'source': source_name,
                'leads': leads_count,
                'status': 'success'
            })
        else:
            print(f"FAILED")
            print(f"   Error: {result.stderr}")
            dry_run_results.append({
                'file': file_path.name,
                'source': source_name,
                'leads': 0,
                'status': 'failed'
            })
    
    # Summary
    print()
    print("=" * 70)
    print("DRY RUN SUMMARY")
    print("=" * 70)
    
    successful = [r for r in dry_run_results if r['status'] == 'success']
    failed = [r for r in dry_run_results if r['status'] == 'failed']
    total_leads = sum(r['leads'] for r in successful)
    
    print(f"Successful:  {len(successful)}/{len(file_list)} files")
    print(f"Failed:      {len(failed)}/{len(file_list)} files")
    print(f"Total leads: {total_leads:,}")
    print("=" * 70)
    print()
    
    if failed:
        print("[WARNING] Failed files:")
        for r in failed:
            print(f"  - {r['file']}")
        print()
    
    if not successful:
        print("[ERROR] No files passed validation. Aborting.")
        sys.exit(1)
    
    # Exit if dry-run-only mode
    if args.dry_run_only:
        print("[DRY RUN ONLY] Stopping here. Use without --dry-run-only to proceed with ingestion.")
        sys.exit(0)
    
    # Phase 2: Confirm and proceed
    if not args.yes:
        print("Ready to proceed with actual ingestion?")
        print(f"  - {len(successful)} files will be ingested")
        print(f"  - {total_leads:,} total leads/owners")
        print(f"  - Appendix data will be SKIPPED")
        print()
        
        confirm = input("Continue? (yes/no): ").strip().lower()
        
        if confirm not in ['yes', 'y']:
            print("[CANCELLED] Ingestion cancelled.")
            sys.exit(0)
    else:
        print("[AUTO-CONFIRM] Proceeding with ingestion...")
        print(f"  - {len(successful)} files will be ingested")
        print(f"  - {total_leads:,} total leads/owners")
        print()
    
    # Phase 3: Actual ingestion
    print()
    print("=" * 70)
    print("PHASE 2: ACTUAL INGESTION")
    print("=" * 70)
    print()
    
    ingested = 0
    
    for i, result in enumerate(dry_run_results, 1):
        if result['status'] != 'success':
            continue
        
        # Find the original file path
        file = [f for f in file_list if Path(f).name == result['file']][0]
        file_path = Path(file)
        
        print(f"[{i}/{len(successful)}] Ingesting: {file_path.name}...", end=' ')
        
        cmd = [
            args.python,
            '-m', 'ingestion.cli',
            '--schema', args.schema,
            '--file', str(file_path.absolute()),
            '--source', result['source'],
            '--mapping-template', args.template,
            '--db', db_path,
            '--skip-appendix'
        ]
        
        result_run = subprocess.run(cmd, capture_output=True, text=True)
        
        if result_run.returncode == 0:
            print(f"OK ({result['leads']:,} leads)")
            ingested += 1
        else:
            print(f"FAILED")
            print(f"   Error: {result_run.stderr}")
    
    # Final summary
    print()
    print("=" * 70)
    print("BATCH INGESTION COMPLETE")
    print("=" * 70)
    print(f"Successfully ingested: {ingested}/{len(successful)} files")
    print(f"Total leads added:     {total_leads:,}")
    print(f"Database:              {db_path}")
    print("=" * 70)
    print()


if __name__ == '__main__':
    main()
