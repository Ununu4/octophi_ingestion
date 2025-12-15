"""
Template-based header mapping for preprocessing.

CLEAN PREPROCESSING FLOW:
1. Load CSV template (incoming → expected mappings)
2. Create normalized lookup dictionary
3. Detect field combinations (first name + last name)
4. Direct O(1) dictionary lookup for each header
5. Apply combinations to merge fields
6. No fuzzy logic, minimal overhead, pure speed

COMBINATION SYNTAX:
    incoming_schema,expected_schema
    first name + last name,owner_name
    
The "+" operator tells the mapper to combine those source columns.
"""
import pandas as pd
from pathlib import Path
from typing import Dict, Optional, List


class TemplateMapper:
    """
    Lightning-fast template-based header mapping.
    
    CORE PRINCIPLE: Explicit is better than implicit.
    When you provide a template, we trust it completely.
    
    Template format:
        incoming_schema,expected_schema
        iusa company name,business_legal_name
        first name + last name,owner_name
        SSN,phone_raw
        ...
    
    Combination syntax:
        Use "+" to combine multiple source columns:
        first name + last name,owner_name
    
    Benefits:
    - O(1) lookup time per header
    - Support for field combinations
    - Zero fuzzy matching overhead
    - 100% accuracy from explicit mappings
    - Predictable, repeatable results
    """
    
    def __init__(self, template_path: str):
        """
        Initialize template mapper.
        
        Args:
            template_path: Path to CSV template file
        """
        self.template_path = Path(template_path)
        self.mapper_type = 'template'  # Identifier for clean flow separation
        self.mapping = {}
        self.combinations = []  # Field combinations (first + last → full)
        self._load_template()
    
    def _load_template(self):
        """
        Load template CSV and create O(1) lookup dictionary.
        
        CLEAN FLOW:
        1. Read CSV template
        2. Validate structure
        3. Detect field combinations (column1 + column2)
        4. Build normalized lookup dict
        """
        if not self.template_path.exists():
            raise FileNotFoundError(f"Template not found: {self.template_path}")
        
        # Read template (fast, no unnecessary processing)
        df = pd.read_csv(self.template_path, dtype=str, keep_default_na=False)
        
        # Validate structure
        required_cols = ['incoming_schema', 'expected_schema']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"Template must have columns: {required_cols}")
        
        # Build mapping dictionary and detect combinations
        for _, row in df.iterrows():
            incoming = str(row['incoming_schema']).strip()
            expected = str(row['expected_schema']).strip()
            
            # Skip empty mappings
            if not incoming or not expected:
                continue
            
            # Check for combination syntax: "field1 + field2"
            if '+' in incoming:
                # Parse combination: "first name + last name" -> ["first name", "last name"]
                sources = [s.strip() for s in incoming.split('+')]
                
                # Store combination rule (use 'sources' to match _apply_combinations)
                self.combinations.append({
                    'target_field': expected,
                    'sources': sources,
                    'separator': ' '  # Default separator
                })
                
                # Mark each source field as "used" so it doesn't go to appendix
                for source in sources:
                    key = source.lower().strip()
                    self.mapping[key] = '__USED_IN_COMBINATION__'
            else:
                # Regular 1:1 mapping
                key = incoming.lower().strip()
                self.mapping[key] = expected
    
    def map_headers(self, raw_headers: List[str]) -> Dict[str, Optional[str]]:
        """
        Map raw headers to canonical fields via direct O(1) lookup.
        
        CORE EFFICIENCY: Dictionary lookup is O(1)
        Total complexity: O(n) where n = number of headers
        
        Args:
            raw_headers: List of raw column headers from CSV
            
        Returns:
            Dictionary: raw_header -> canonical_field (or None if unmapped)
        """
        # Direct dictionary lookup - no fuzzy matching overhead
        result = {}
        for header in raw_headers:
            key = header.lower().strip()
            result[header] = self.mapping.get(key)  # O(1) lookup
        
        return result
    
    def get_mapped_count(self, raw_headers: List[str]) -> int:
        """
        Count how many headers will be mapped.
        
        Args:
            raw_headers: List of raw column headers
            
        Returns:
            Number of headers that have mappings
        """
        count = 0
        for header in raw_headers:
            normalized = header.lower().strip()
            if normalized in self.mapping:
                count += 1
        return count
    
    def get_unmapped_headers(self, raw_headers: List[str]) -> List[str]:
        """
        Get list of headers that don't have mappings.
        
        Args:
            raw_headers: List of raw column headers
            
        Returns:
            List of unmapped headers
        """
        unmapped = []
        for header in raw_headers:
            normalized = header.lower().strip()
            if normalized not in self.mapping:
                unmapped.append(header)
        return unmapped
    
    def get_mapping_summary(self) -> Dict[str, str]:
        """
        Get the complete mapping dictionary.
        
        Returns:
            Dictionary of all mappings in template
        """
        return self.mapping.copy()
    
    def get_combinations(self) -> List[Dict]:
        """
        Get field combination rules from template.
        
        Template syntax: "first name + last name,owner_name"
        
        Returns:
            List of combination rules with target_field, source_fields, separator
        """
        return self.combinations
    
    def get_computations(self) -> List[Dict]:
        """
        Get field computation rules.
        
        For template-based mapping, computations are not supported
        since templates provide explicit mappings only.
        
        Returns:
            Empty list (no computations in template mode)
        """
        return []

