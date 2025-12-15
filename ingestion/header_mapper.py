"""
Header Mapper Module

Maps messy input CSV headers to canonical schema fields using fuzzy matching.
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Optional


class HeaderMapper:
    """Maps raw CSV headers to canonical field names."""
    
    def __init__(self, fuzzy_map_path: str, combinations_path: str = None, computations_path: str = None):
        """
        Initialize header mapper.
        
        Args:
            fuzzy_map_path: Path to fuzzy.json file
            combinations_path: Path to fuzzy_combinations.json file (optional)
            computations_path: Path to fuzzy_computations.json file (optional)
        """
        self.path = Path(fuzzy_map_path)
        self.fuzzy_map = self._load_fuzzy_map()
        # Create reverse index: normalized_variant -> canonical_field
        self.reverse_map = self._build_reverse_map()
        self.mapper_type = 'fuzzy'  # Intelligent fuzzy mode identifier
        
        # Load combination rules
        if combinations_path is None:
            combinations_path = self.path.parent / 'fuzzy_combinations.json'
        self.combinations = self._load_combinations(Path(combinations_path))
        self.combination_matches = []  # Track detected combinations
        
        # Load computation rules
        if computations_path is None:
            computations_path = self.path.parent / 'fuzzy_computations.json'
        self.computations = self._load_computations(Path(computations_path))
        self.computation_matches = []  # Track detected computations
        
    def _load_fuzzy_map(self) -> dict:
        """Load fuzzy mapping from JSON file."""
        if not self.path.exists():
            raise FileNotFoundError(f"Fuzzy map file not found: {self.path}")
        
        with open(self.path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _load_combinations(self, path: Path) -> list:
        """Load field combination rules."""
        if not path.exists():
            print(f"⚠️  No combination rules found at {path}, skipping...")
            return []
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('combinations', [])
        except Exception as e:
            print(f"⚠️  Error loading combinations: {e}")
            return []
    
    def _load_computations(self, path: Path) -> list:
        """Load field computation rules."""
        if not path.exists():
            print(f"⚠️  No computation rules found at {path}, skipping...")
            return []
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('computations', [])
        except Exception as e:
            print(f"⚠️  Error loading computations: {e}")
            return []
    
    def _build_reverse_map(self) -> Dict[str, str]:
        """
        Build reverse mapping from normalized variants to canonical fields.
        
        Returns:
            Dict mapping normalized variant -> canonical field name
        """
        reverse = {}
        for canonical_field, variants in self.fuzzy_map.items():
            for variant in variants:
                normalized = self._normalize_header(variant)
                reverse[normalized] = canonical_field
        return reverse
    
    def _normalize_header(self, header: str) -> str:
        """
        Normalize a header string for matching.
        
        Args:
            header: Raw header string
            
        Returns:
            Normalized header (lowercase, no spaces/symbols)
        """
        if not header:
            return ""
        
        # Convert to lowercase
        normalized = header.lower()
        
        # Strip whitespace
        normalized = normalized.strip()
        
        # Remove common symbols and replace with single space
        normalized = re.sub(r'[_\-\./\\]+', ' ', normalized)
        
        # Remove all remaining non-alphanumeric except spaces
        normalized = re.sub(r'[^a-z0-9\s]', '', normalized)
        
        # Collapse multiple spaces
        normalized = re.sub(r'\s+', ' ', normalized)
        
        # Remove spaces for final comparison
        normalized = normalized.replace(' ', '')
        
        return normalized
    
    def map_headers(self, raw_headers: List[str]) -> Dict[str, Optional[str]]:
        """
        Map raw headers to canonical field names.
        
        Args:
            raw_headers: List of raw header strings from CSV
            
        Returns:
            Dict mapping raw_header -> canonical_field (or None if no match)
        """
        mapping = {}
        
        for raw_header in raw_headers:
            normalized = self._normalize_header(raw_header)
            
            # Try exact normalized match first
            if normalized in self.reverse_map:
                mapping[raw_header] = self.reverse_map[normalized]
            else:
                # No match - mark as appendix (None)
                mapping[raw_header] = None
        
        # Detect and mark combination opportunities
        self._detect_combinations(raw_headers, mapping)
        
        # Detect and mark computation opportunities
        self._detect_computations(raw_headers, mapping)
        
        return mapping
    
    def _detect_combinations(self, raw_headers: List[str], mapping: Dict[str, Optional[str]]):
        """
        Detect if raw headers match combination rules.
        
        Args:
            raw_headers: List of raw header strings
            mapping: Current header mapping
        """
        self.combination_matches = []
        
        for rule in self.combinations:
            # Check if all source patterns are present in raw headers
            matched_sources = {}
            
            for source_def in rule['sources']:
                pattern = source_def['pattern']
                order = source_def['order']
                
                # Find matching header
                for raw_header in raw_headers:
                    normalized = self._normalize_header(raw_header)
                    normalized_pattern = self._normalize_header(pattern)
                    
                    if normalized == normalized_pattern:
                        matched_sources[order] = raw_header
                        break
            
            # If all sources matched, record the combination
            if len(matched_sources) == len(rule['sources']):
                combination_match = {
                    'target_field': rule['target_field'],
                    'type': rule['type'],
                    'sources': [matched_sources[src['order']] for src in sorted(rule['sources'], key=lambda x: x['order'])],
                    'separator': rule.get('separator', ' '),
                    'description': rule.get('description', '')
                }
                self.combination_matches.append(combination_match)
                
                # Mark source columns as "used for combination"
                for source_header in combination_match['sources']:
                    mapping[source_header] = f"_COMBINE_{rule['target_field']}"
    
    def get_combinations(self) -> List[Dict]:
        """
        Get detected field combinations.
        
        Returns:
            List of combination instructions
        """
        return self.combination_matches
    
    def _detect_computations(self, raw_headers: List[str], mapping: Dict[str, Optional[str]]):
        """
        Detect if raw headers match computation rules.
        
        Args:
            raw_headers: List of raw header strings
            mapping: Current header mapping
        """
        self.computation_matches = []
        
        for rule in self.computations:
            # Check if any source pattern matches a header
            matched_source = None
            
            for source_def in rule['sources']:
                pattern = source_def['pattern']
                
                for raw_header in raw_headers:
                    normalized = self._normalize_header(raw_header)
                    normalized_pattern = self._normalize_header(pattern)
                    
                    if normalized == normalized_pattern:
                        matched_source = raw_header
                        break
                
                if matched_source:
                    break
            
            # If source matched, record the computation
            if matched_source:
                computation_match = {
                    'target_field': rule['target_field'],
                    'type': rule['type'],
                    'source': matched_source,
                    'computation': rule['computation'],
                    'description': rule.get('description', '')
                }
                self.computation_matches.append(computation_match)
                
                # Mark source column as "used for computation"
                mapping[matched_source] = f"_COMPUTE_{rule['target_field']}"
    
    def get_computations(self) -> List[Dict]:
        """
        Get detected field computations.
        
        Returns:
            List of computation instructions
        """
        return self.computation_matches
    
    def get_canonical_field(self, raw_header: str) -> Optional[str]:
        """
        Get canonical field name for a single raw header.
        
        Args:
            raw_header: Raw header string
            
        Returns:
            Canonical field name or None if no match
        """
        normalized = self._normalize_header(raw_header)
        return self.reverse_map.get(normalized)
    
    def is_known_field(self, raw_header: str) -> bool:
        """
        Check if a raw header maps to a known field.
        
        Args:
            raw_header: Raw header string
            
        Returns:
            True if header maps to schema field, False otherwise
        """
        return self.get_canonical_field(raw_header) is not None
    
    def get_unmapped_headers(self, raw_headers: List[str]) -> List[str]:
        """
        Get list of headers that don't map to schema fields.
        
        Args:
            raw_headers: List of raw header strings
            
        Returns:
            List of unmapped headers (appendix candidates)
        """
        unmapped = []
        for header in raw_headers:
            if not self.is_known_field(header):
                unmapped.append(header)
        return unmapped



