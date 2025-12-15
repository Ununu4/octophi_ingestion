"""
File Operations Module

Utility functions for file operations.
"""

from pathlib import Path
from typing import Optional


def validate_file_path(file_path: str, required_extensions: Optional[list] = None) -> Path:
    """
    Validate that a file exists and has the correct extension.
    
    Args:
        file_path: Path to file
        required_extensions: List of allowed extensions (e.g., ['.csv', '.xlsx'])
        
    Returns:
        Path object
        
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If extension is not allowed
    """
    path = Path(file_path)
    
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    if not path.is_file():
        raise ValueError(f"Not a file: {file_path}")
    
    if required_extensions:
        if path.suffix.lower() not in [ext.lower() for ext in required_extensions]:
            raise ValueError(
                f"Invalid file extension: {path.suffix}. "
                f"Allowed: {', '.join(required_extensions)}"
            )
    
    return path


def ensure_directory(dir_path: str) -> Path:
    """
    Ensure a directory exists, create if it doesn't.
    
    Args:
        dir_path: Path to directory
        
    Returns:
        Path object
    """
    path = Path(dir_path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_file_size_mb(file_path: str) -> float:
    """
    Get file size in megabytes.
    
    Args:
        file_path: Path to file
        
    Returns:
        File size in MB
    """
    path = Path(file_path)
    if not path.exists():
        return 0.0
    return path.stat().st_size / (1024 * 1024)




