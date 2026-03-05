"""
File utilities for handling file storage, naming, and path generation.
"""
import os
import re
import unicodedata
import uuid
from pathlib import Path
from typing import Tuple


def slugify(text: str) -> str:
    """
    Convert text to a slug suitable for filenames and paths.
    
    - Lowercase
    - Remove accents/diacritics
    - Keep only [a-z0-9_]
    - Convert spaces and hyphens to underscore
    - Reduce multiple underscores to single
    - Strip leading/trailing underscores
    
    Args:
        text: Input string to slugify
        
    Returns:
        Slugified string safe for filenames
        
    Examples:
        >>> slugify("Jan-Willem de Groot")
        'jan_willem_de_groot'
        >>> slugify("François Müller")
        'francois_muller'
        >>> slugify("Test___Multiple---Spaces")
        'test_multiple_spaces'
    """
    if not text:
        return ""
    
    # Normalize unicode characters and remove accents
    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ASCII', 'ignore').decode('ASCII')
    
    # Lowercase
    text = text.lower()
    
    # Replace spaces and hyphens with underscore
    text = re.sub(r'[\s\-]+', '_', text)
    
    # Keep only alphanumeric and underscore
    text = re.sub(r'[^a-z0-9_]', '', text)
    
    # Reduce multiple underscores to single
    text = re.sub(r'_+', '_', text)
    
    # Strip leading/trailing underscores
    text = text.strip('_')
    
    return text


def generate_filename(
    entity_id: int,
    document_type: str,
    year: int = None,
    extension: str = "pdf"
) -> str:
    """
    Generate a unique filename following the pattern:
    <entity_id>_<documenttype>_<jaar>_<uuid>.<ext>
    
    If year is not provided or is 0, it will be omitted from the filename.
    
    Args:
        entity_id: Person ID or family ID (father_id_mother_id)
        document_type: Type of document (e.g., 'portret', 'geboorteakte')
        year: Optional year associated with the document
        extension: File extension without dot (default: 'pdf')
        
    Returns:
        Generated filename
        
    Examples:
        >>> generate_filename(123, 'portret', 1950, 'jpg')  # doctest: +SKIP
        '123_portret_1950_a1b2c3d4.jpg'
        >>> generate_filename(123, 'geboorteakte', extension='pdf')  # doctest: +SKIP
        '123_geboorteakte_e5f6g7h8.pdf'
    """
    # Generate UUID (take first 8 characters for brevity)
    unique_id = str(uuid.uuid4()).split('-')[0]
    
    # Clean extension
    extension = extension.lstrip('.')
    
    # Slugify document type
    doc_type_slug = slugify(document_type)
    
    # Build filename parts
    if year and year > 0:
        filename = f"{entity_id}_{doc_type_slug}_{year}_{unique_id}.{extension}"
    else:
        filename = f"{entity_id}_{doc_type_slug}_{unique_id}.{extension}"
    
    return filename


def get_person_path(
    base_path: str,
    person_id: int,
    first_name: str,
    last_name: str
) -> Path:
    """
    Generate storage path for a person:
    <base_path>/<person_id>/<slugified_naam>/
    
    Args:
        base_path: Base storage directory
        person_id: Unique person identifier
        first_name: Person's first name
        last_name: Person's last name
        
    Returns:
        Path object for person's storage directory
        
    Examples:
        >>> get_person_path('/data', 123, 'Jan', 'de Vries')
        PosixPath('/data/123/jan_de_vries')
    """
    slugified_name = slugify(f"{first_name} {last_name}")
    return Path(base_path) / str(person_id) / slugified_name


def get_family_path(
    base_path: str,
    father_id: int,
    father_first_name: str,
    father_last_name: str,
    mother_id: int,
    mother_first_name: str,
    mother_last_name: str
) -> Path:
    """
    Generate storage path for a family (parent couple):
    <base_path>/<vader_id>_<moeder_id>/<slugified_vadernaam>_<slugified_moedernaam>/
    
    Args:
        base_path: Base storage directory
        father_id: Father's person ID
        father_first_name: Father's first name
        father_last_name: Father's last name
        mother_id: Mother's person ID
        mother_first_name: Mother's first name
        mother_last_name: Mother's last name
        
    Returns:
        Path object for family's storage directory
        
    Examples:
        >>> get_family_path('/data', 123, 'Jan', 'Bakker', 456, 'Marie', 'Jansen')
        PosixPath('/data/123_456/jan_bakker_marie_jansen')
    """
    father_slug = slugify(f"{father_first_name} {father_last_name}")
    mother_slug = slugify(f"{mother_first_name} {mother_last_name}")
    
    family_id = f"{father_id}_{mother_id}"
    family_name = f"{father_slug}_{mother_slug}"
    
    return Path(base_path) / family_id / family_name


def ensure_directory_exists(path: Path) -> None:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        path: Directory path to ensure exists
        
    Raises:
        OSError: If directory cannot be created
    """
    path.mkdir(parents=True, exist_ok=True)


def get_storage_base_path(environment: str = "development") -> str:
    """
    Get the base storage path based on environment.
    
    Args:
        environment: Either 'development' or 'production'
        
    Returns:
        Base path for file storage
    """
    if environment == "production":
        # Production: Synology NAS
        return os.getenv(
            "STORAGE_BASE_PATH",
            "/docker/familiez/media"
        )
    else:
        # Development: Local machine
        return os.getenv(
            "STORAGE_BASE_PATH",
            "/home/frans/Documenten/Dev/Familiez/BESTANDEN"
        )
