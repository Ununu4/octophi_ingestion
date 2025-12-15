"""
Duration Parser Module

Parses duration strings and converts them to dates.
"""

import re
from datetime import datetime, timedelta
from typing import Optional


class DurationParser:
    """Parses duration strings and computes dates."""
    
    @staticmethod
    def parse_duration_to_date(duration_str: str, reference_date: datetime = None) -> Optional[str]:
        """
        Parse a duration string and compute the start date.
        
        Args:
            duration_str: Duration string like "10 years", "36+ months", "2years + in businesses"
            reference_date: Reference date to subtract from (default: today)
            
        Returns:
            Date string in YYYY-MM-DD format, or None if parsing fails
        """
        if not duration_str:
            return None
        
        if reference_date is None:
            reference_date = datetime.now()
        
        duration_str = str(duration_str).lower().strip()
        
        # Handle placeholder values
        if duration_str in ['na', 'n/a', 'none', 'null', 'unknown', '']:
            return None
        
        # Extract years
        years_match = re.search(r'(\d+)\s*(?:\+)?\s*(?:year|yr)', duration_str)
        if years_match:
            years = int(years_match.group(1))
            start_date = reference_date - timedelta(days=years * 365)
            return start_date.strftime('%Y-%m-%d')
        
        # Extract months
        months_match = re.search(r'(\d+)\s*(?:\+)?\s*(?:month|mo)', duration_str)
        if months_match:
            months = int(months_match.group(1))
            # Approximate months as 30 days
            start_date = reference_date - timedelta(days=months * 30)
            return start_date.strftime('%Y-%m-%d')
        
        # Extract just a number (assume years)
        number_match = re.search(r'(\d+)', duration_str)
        if number_match:
            years = int(number_match.group(1))
            # If number is > 12, likely months; otherwise years
            if years > 12:
                start_date = reference_date - timedelta(days=(years * 30))
            else:
                start_date = reference_date - timedelta(days=years * 365)
            return start_date.strftime('%Y-%m-%d')
        
        return None
    
    @staticmethod
    def extract_years(duration_str: str) -> Optional[float]:
        """
        Extract years from duration string.
        
        Args:
            duration_str: Duration string
            
        Returns:
            Number of years as float, or None
        """
        if not duration_str:
            return None
        
        duration_str = str(duration_str).lower()
        
        # Try years first
        years_match = re.search(r'(\d+)\s*(?:\+)?\s*(?:year|yr)', duration_str)
        if years_match:
            return float(years_match.group(1))
        
        # Try months
        months_match = re.search(r'(\d+)\s*(?:\+)?\s*(?:month|mo)', duration_str)
        if months_match:
            return float(months_match.group(1)) / 12.0
        
        return None


# Convenience function
def duration_to_date(duration_str: str) -> Optional[str]:
    """
    Convert duration string to start date.
    
    Args:
        duration_str: Duration like "10 years", "36 months"
        
    Returns:
        Date string YYYY-MM-DD or None
    """
    return DurationParser.parse_duration_to_date(duration_str)


