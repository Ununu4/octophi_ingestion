"""
Type Normalizer Module

Applies type-specific normalization to field values.
"""

import re
from datetime import datetime
from typing import Optional


class Normalizer:
    """Normalizes values based on field types."""
    
    # SIC to SOC placeholder mapping (can be enhanced later)
    SIC_TO_SOC_MAP = {
        # This is a placeholder - in production, this would be a comprehensive mapping
        # For now, we'll just pass through SIC as SOC
    }
    
    def __init__(self):
        """Initialize normalizer."""
        pass
    
    def normalize(self, value: any, type_name: str) -> Optional[str]:
        """
        Normalize a value based on its type.
        
        Args:
            value: Raw value to normalize
            type_name: Type name from schema (e.g., 'phone', 'email', 'state')
            
        Returns:
            Normalized value as string, or None if value is empty/invalid
        """
        # Handle None or empty values
        if value is None or value == '':
            return None
        
        # Convert to string if not already
        value_str = str(value).strip()
        
        # Check for placeholder values
        if self._is_placeholder(value_str):
            return None
        
        # Apply type-specific normalization
        normalizer_method = f"_normalize_{type_name}"
        if hasattr(self, normalizer_method):
            return getattr(self, normalizer_method)(value_str)
        else:
            # Default: just strip whitespace
            return self._normalize_string(value_str)
    
    def _is_placeholder(self, value: str) -> bool:
        """Check if value is a placeholder (NA, NULL, etc.)."""
        placeholders = {
            'na', 'n/a', 'none', 'null', 'nil', 
            'unknown', 'unspecified', 'tbd', 'nan', ''
        }
        return value.lower() in placeholders
    
    def _normalize_string(self, value: str) -> Optional[str]:
        """Normalize generic string."""
        value = value.strip()
        return value if value else None
    
    def _normalize_phone(self, value: str) -> Optional[str]:
        """Normalize phone number to digits only."""
        # Extract only digits
        digits = re.sub(r'\D', '', value)
        return digits if digits else None
    
    def _normalize_phone_clean(self, value: str) -> Optional[str]:
        """
        Normalize cleaned phone number.
        This is typically derived from phone_raw.
        """
        return self._normalize_phone(value)
    
    def _normalize_zip(self, value: str) -> Optional[str]:
        """Normalize ZIP code to first 5 digits."""
        digits = re.sub(r'\D', '', value)
        if digits:
            # Take first 5 digits
            return digits[:5]
        return None
    
    def _normalize_state(self, value: str) -> Optional[str]:
        """Normalize state to uppercase 2-letter code."""
        value = value.strip().upper()
        # Only return if it's 2 characters (valid state code)
        if len(value) == 2 and value.isalpha():
            return value
        # Handle longer state names - just uppercase for now
        return value if value else None
    
    def _normalize_email(self, value: str) -> Optional[str]:
        """Normalize email to lowercase."""
        value = value.strip().lower()
        # Basic email validation
        if '@' in value and '.' in value:
            return value
        return None
    
    def _normalize_id_number(self, value: str) -> Optional[str]:
        """Normalize ID numbers (SSN, EIN, Tax ID) to digits only."""
        digits = re.sub(r'\D', '', value)
        return digits if digits else None
    
    def _normalize_date(self, value: str) -> Optional[str]:
        """
        Normalize date to ISO format (YYYY-MM-DD).
        Best-effort parsing.
        """
        if not value:
            return None
        
        # Try common date formats
        date_formats = [
            '%Y-%m-%d',           # 2020-01-15
            '%m/%d/%Y',           # 01/15/2020
            '%m-%d-%Y',           # 01-15-2020
            '%d/%m/%Y',           # 15/01/2020
            '%Y/%m/%d',           # 2020/01/15
            '%m/%d/%y',           # 01/15/20
            '%Y%m%d',             # 20200115
        ]
        
        for fmt in date_formats:
            try:
                dt = datetime.strptime(value, fmt)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        # If all parsing fails, return original if it looks date-like
        if re.match(r'\d{4}-\d{2}-\d{2}', value):
            return value
        
        return None
    
    def _normalize_datetime(self, value: str) -> Optional[str]:
        """
        Normalize datetime to ISO format.
        System-generated fields typically use this.
        """
        if not value:
            return None
        
        # If it's already in ISO format, return it
        if re.match(r'\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}', value):
            return value
        
        # Try parsing
        datetime_formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%m/%d/%Y %H:%M:%S',
        ]
        
        for fmt in datetime_formats:
            try:
                dt = datetime.strptime(value, fmt)
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                continue
        
        return None
    
    def _normalize_sic_code(self, value: str) -> Optional[str]:
        """Normalize SIC code (ensure digits)."""
        # SIC codes are typically numeric
        digits = re.sub(r'\D', '', value)
        return digits if digits else value.strip() or None
    
    def _normalize_soc_code(self, value: str) -> Optional[str]:
        """
        Normalize SOC code.
        This is typically derived from SIC, but can be provided directly.
        """
        value = value.strip()
        return value if value else None
    
    def _normalize_address(self, value: str) -> Optional[str]:
        """Normalize address (just strip for now)."""
        return self._normalize_string(value)
    
    def _normalize_person_name(self, value: str) -> Optional[str]:
        """Normalize person name."""
        value = value.strip()
        # Basic name normalization - title case
        if value:
            # Remove extra spaces
            value = re.sub(r'\s+', ' ', value)
            return value
        return None
    
    def derive_phone_clean(self, phone_raw: Optional[str]) -> Optional[str]:
        """
        Derive phone_clean from phone_raw.
        
        Args:
            phone_raw: Raw phone number
            
        Returns:
            Cleaned phone number (digits only)
        """
        if not phone_raw:
            return None
        return self._normalize_phone(phone_raw)
    
    def derive_soc_from_sic(self, sic: Optional[str]) -> Optional[str]:
        """
        Derive SOC code from SIC code.
        
        Args:
            sic: SIC code
            
        Returns:
            SOC code (mapped from SIC or passed through)
        """
        if not sic:
            return None
        
        # Check if we have a mapping
        if sic in self.SIC_TO_SOC_MAP:
            return self.SIC_TO_SOC_MAP[sic]
        
        # For now, just pass through
        # In production, implement full SIC->SOC mapping
        return sic




