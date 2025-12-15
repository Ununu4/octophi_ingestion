# Installation Guide

## Prerequisites

- Python 3.9 or higher
- pip (Python package manager)

## Installation Steps

### 1. Install Dependencies

The system requires only pandas and openpyxl:

```bash
cd octophi_ingestion
pip install -r requirements.txt
```

Or install manually:

```bash
pip install pandas>=2.0.0 openpyxl>=3.1.0
```

### 2. Verify Installation

Run the verification script:

```bash
python verify_setup.py
```

Expected output:

```
======================================================================
🐙 OCTOPHI INGESTION SYSTEM V1 - SETUP VERIFICATION
======================================================================

1. Testing module imports...
   ✓ All modules imported successfully

2. Testing schema loading...
   ✓ Schema: monet_merchant v1.0
   ...

✅ ALL VERIFICATION CHECKS PASSED!
======================================================================
```

### 3. Test CLI

```bash
python -m ingestion.cli --help
```

## Troubleshooting Installation

### Issue: pip not found

**Windows:**
```powershell
python -m pip install pandas openpyxl
```

**Linux/Mac:**
```bash
python3 -m pip install pandas openpyxl
```

### Issue: Permission denied

Use `--user` flag:
```bash
pip install --user pandas openpyxl
```

### Issue: Module not found when running

Make sure you're in the correct directory:
```bash
cd octophi_ingestion
python -m ingestion.cli --help
```

## That's It!

You're ready to use OCTOPHI. See `USAGE_GUIDE.md` for usage examples.




