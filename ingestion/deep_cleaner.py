"""
Deep Cleaner Module

Takes raw CSV/XLSX files and produces clean, normalized DataFrames
that conform to the schema, with extra columns routed to appendix.
"""

import pandas as pd
from pathlib import Path
from typing import Tuple, List, Dict, Optional
from .schema_loader import Schema
from .type_normalizer import Normalizer


class DeepCleaner:
    """Cleans and normalizes raw data files. Template-based mapping only."""

    def __init__(self, schema: Schema, mapper, normalizer: Normalizer):
        """
        Initialize deep cleaner.
        
        Args:
            schema: Schema loader instance
            mapper: Header mapper instance
            normalizer: Normalizer instance
        """
        self.schema = schema
        self.mapper = mapper
        self.normalizer = normalizer
    
    def clean_file(self, input_path: str, upload_tag: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Clean a CSV or XLSX file.
        
        Args:
            input_path: Path to input file
            upload_tag: Tag for this upload (e.g., timestamp)
            
        Returns:
            Tuple of (clean_leads_df, clean_owners_df, appendix_df)
        """
        # Load file
        df = self._load_file(input_path)
        
        # Map headers (template mapper only)
        header_mapping = self.mapper.map_headers(df.columns.tolist())

        # Apply field combinations if defined in template
        combinations = self.mapper.get_combinations()
        if combinations:
            df, header_mapping = self._apply_combinations(df, combinations, header_mapping)

        # Identify known vs unknown columns
        known_cols = {raw: canonical for raw, canonical in header_mapping.items()
                      if canonical is not None and canonical != '__USED_IN_COMBINATION__'}
        unknown_cols = [raw for raw, canonical in header_mapping.items()
                        if canonical is None]

        # Remove combination sources from unknown (they're used, not appendix)
        combination_sources = []
        for combo in combinations:
            combination_sources.extend(combo['sources'])
        unknown_cols = [col for col in unknown_cols if col not in combination_sources]

        # Exclude specific columns from appendix (e.g., ZB Status)
        exclude_from_appendix = ['ZB Status', 'zb status', 'ZB Status ']
        unknown_cols = [col for col in unknown_cols if col not in exclude_from_appendix]

        print(f"[LOAD] {len(df)} rows with {len(df.columns)} columns")
        print(f"[DIRECT MAP] Mapped {len(known_cols)} fields via template")
        if combinations:
            print(f"[COMBINE] Applied {len(combinations)} field combination(s):")
            for combo in combinations:
                print(f"   - {' + '.join(combo['sources'])} -> {combo['target_field']}")
        if unknown_cols:
            print(f"[APPENDIX] {len(unknown_cols)} unmapped columns: {', '.join(unknown_cols)}")

        # Rename known columns to canonical names
        df_renamed = df.rename(columns=known_cols)
        
        # Get lead and owner fields from schema
        lead_fields = self.schema.fields('lead')
        owner_fields = self.schema.fields('owner')
        
        # Create lead DataFrame
        leads_df = self._create_entity_df(df_renamed, lead_fields, 'lead')
        
        # Create owner DataFrame
        owners_df = self._create_entity_df(df_renamed, owner_fields, 'owner')
        
        # Create appendix DataFrame
        appendix_df = self._create_appendix_df(df, unknown_cols, upload_tag)
        
        print(f"[OK] Cleaning complete: {len(leads_df)} leads, {len(owners_df)} owners, {len(appendix_df)} appendix rows")
        
        return leads_df, owners_df, appendix_df
    
    def _load_file(self, input_path: str) -> pd.DataFrame:
        """Load CSV or XLSX file."""
        path = Path(input_path)
        
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")
        
        # Check file extension
        ext = path.suffix.lower()
        
        if ext == '.csv':
            return pd.read_csv(path, dtype=str, keep_default_na=False)
        elif ext in ['.xlsx', '.xls']:
            return pd.read_excel(path, dtype=str, keep_default_na=False)
        else:
            raise ValueError(f"Unsupported file format: {ext}. Use .csv or .xlsx")
    
    def _create_entity_df(self, df: pd.DataFrame, field_list: List[str], entity: str) -> pd.DataFrame:
        """
        Create a clean DataFrame for an entity (lead or owner).
        
        Args:
            df: Source DataFrame with canonically-named columns
            field_list: List of fields for this entity from schema
            entity: Entity name ('lead' or 'owner')
            
        Returns:
            Clean DataFrame with exact schema fields
        """
        result = pd.DataFrame()
        
        for field in field_list:
            # Skip system-generated fields
            if self.schema.is_system_generated(entity, field):
                continue
            
            # Check if field is derived
            source_field = self.schema.derived_from(entity, field)
            
            if source_field:
                # Derive from source field
                if source_field in df.columns:
                    source_values = df[source_field]
                    # Apply derivation logic
                    if field.endswith('_clean') and source_field.endswith('_raw'):
                        # Phone clean derivation
                        result[field] = source_values.apply(lambda x: self.normalizer.derive_phone_clean(x))
                    elif field == 'soc' and source_field == 'sic':
                        # SOC derivation from SIC
                        result[field] = source_values.apply(lambda x: self.normalizer.derive_soc_from_sic(x))
                    else:
                        # Generic derivation - just normalize with target type
                        field_type = self.schema.field_type(entity, field)
                        result[field] = source_values.apply(lambda x: self.normalizer.normalize(x, field_type))
                else:
                    # Source field not present - fill with None
                    result[field] = None
            else:
                # Regular field
                if field in df.columns:
                    field_type = self.schema.field_type(entity, field)
                    
                    # Special handling for start_date: convert TIB (Time In Business) to actual date
                    if field == 'start_date' and field_type == 'date':
                        result[field] = df[field].apply(lambda x: self._convert_tib_to_date(x))
                    else:
                        # Normalize based on type
                        result[field] = df[field].apply(lambda x: self.normalizer.normalize(x, field_type))
                else:
                    # Field not present in input - fill with None
                    result[field] = None
        
        return result
    
    def _convert_tib_to_date(self, value: any) -> Optional[str]:
        """
        Convert Time In Business (TIB) numeric value to actual start date.
        
        If value is numeric (e.g., 12 meaning 12 years in business),
        calculate the start date as: current_year - TIB_value.
        
        If value is already a date string, pass it through to the normalizer.
        
        Args:
            value: TIB value (numeric or date string)
            
        Returns:
            ISO date string (YYYY-MM-DD) or None
        """
        if value is None or value == '':
            return None
        
        value_str = str(value).strip()
        
        # Check if it's already a date format (contains '-' or '/')
        if '-' in value_str or '/' in value_str:
            # Looks like a date, pass to normalizer
            return self.normalizer.normalize(value_str, 'date')
        
        # Try to parse as numeric (years in business)
        try:
            tib_years = float(value_str)
            
            # Sanity check: TIB should be between 0 and 100 years
            if 0 <= tib_years <= 100:
                from datetime import datetime
                current_year = datetime.now().year
                start_year = int(current_year - tib_years)
                # Return as YYYY-01-01 (default to January 1st)
                return f"{start_year}-01-01"
        except (ValueError, TypeError):
            # Not numeric, try passing to normalizer as-is
            pass
        
        # Last resort: pass to date normalizer
        return self.normalizer.normalize(value_str, 'date')
    
    def _create_appendix_df(self, df: pd.DataFrame, extra_columns: List[str], upload_tag: str) -> pd.DataFrame:
        """
        Create appendix DataFrame from extra columns.
        
        Args:
            df: Original DataFrame with raw column names
            extra_columns: List of column names not in schema
            upload_tag: Upload tag for this batch
            
        Returns:
            DataFrame with columns: lead_id_placeholder, column_name, value, original_row_number, upload_tag
        """
        if not extra_columns:
            # No extra columns - return empty DataFrame
            return pd.DataFrame(columns=['lead_id_placeholder', 'column_name', 'value', 'original_row_number', 'upload_tag'])
        
        rows = []
        
        for idx, row in df.iterrows():
            for col in extra_columns:
                value = row[col]
                # Only store non-empty values
                if value and str(value).strip():
                    rows.append({
                        'lead_id_placeholder': idx,  # Will be replaced with actual lead_id after insert
                        'column_name': col,
                        'value': str(value).strip(),
                        'original_row_number': idx + 1,  # 1-indexed for human readability
                        'upload_tag': upload_tag
                    })
        
        return pd.DataFrame(rows)
    
    def _apply_combinations(self, df: pd.DataFrame, combinations: List[Dict], 
                           header_mapping: Dict) -> Tuple[pd.DataFrame, Dict]:
        """
        Apply field combination rules to DataFrame.
        
        Args:
            df: Source DataFrame
            combinations: List of combination rules from header mapper
            header_mapping: Current header mapping dict
            
        Returns:
            Tuple of (modified DataFrame, updated header mapping)
        """
        df = df.copy()
        
        for combo in combinations:
            target_field = combo['target_field']
            sources = combo['sources']
            separator = combo['separator']
            
            # Find actual column names (case-insensitive, whitespace-tolerant)
            actual_sources = []
            for source in sources:
                # Create normalized lookup map
                normalized_cols = {col.lower().strip(): col for col in df.columns}
                source_normalized = source.lower().strip()
                
                if source_normalized in normalized_cols:
                    actual_sources.append(normalized_cols[source_normalized])
                else:
                    raise ValueError(f"Source column '{source}' not found in CSV. Available: {list(df.columns)}")
            
            # Create combined column using actual column names
            combined_values = df[actual_sources[0]].fillna('')
            for source in actual_sources[1:]:
                combined_values = combined_values + separator + df[source].fillna('')
            
            # Strip extra whitespace
            combined_values = combined_values.str.strip()
            
            # Add combined column to DataFrame
            df[target_field] = combined_values
            
            # Update header mapping to include the new combined field
            header_mapping[target_field] = target_field
        
        return df, header_mapping

    def validate_required_fields(self, leads_df: pd.DataFrame, owners_df: pd.DataFrame) -> List[str]:
        """
        Validate that required fields are present and not empty.
        
        Args:
            leads_df: Leads DataFrame
            owners_df: Owners DataFrame
            
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Check lead required fields
        for field in self.schema.fields('lead'):
            if self.schema.is_required('lead', field):
                # Skip 'source' field - it's provided via CLI, not from CSV
                if field == 'source':
                    continue
                    
                if field not in leads_df.columns:
                    errors.append(f"Required lead field missing: {field}")
                elif leads_df[field].isna().all():
                    errors.append(f"Required lead field is all empty: {field}")
        
        # Check owner required fields (typically none, but check anyway)
        for field in self.schema.fields('owner'):
            if self.schema.is_required('owner', field):
                if field not in owners_df.columns:
                    errors.append(f"Required owner field missing: {field}")
                elif owners_df[field].isna().all():
                    errors.append(f"Required owner field is all empty: {field}")
        
        return errors

