"""
Tests for file_utils module.
"""
import pytest
from pathlib import Path
from file_utils import (
    slugify,
    generate_filename,
    get_person_path,
    get_family_path,
    ensure_directory_exists,
    get_storage_base_path
)


class TestSlugify:
    """Tests for the slugify function."""
    
    def test_simple_text(self):
        """Test basic text conversion."""
        assert slugify("Hello World") == "hello_world"
        assert slugify("Test123") == "test123"
    
    def test_accents_removed(self):
        """Test that accents and diacritics are removed."""
        assert slugify("François") == "francois"
        assert slugify("Müller") == "muller"
        assert slugify("José") == "jose"
        # Note: some special characters like ø may not convert perfectly
        # but the function handles most common European accents
    
    def test_special_characters(self):
        """Test that special characters are handled."""
        # Special characters without adjacent spaces are simply removed
        assert slugify("Hello@World!") == "helloworld"
        assert slugify("Test#123$") == "test123"
        # Special characters with spaces become underscores
        assert slugify("a & b") == "a_b"
    
    def test_hyphens_and_spaces(self):
        """Test conversion of hyphens and spaces to underscores."""
        assert slugify("Jan-Willem") == "jan_willem"
        assert slugify("de Groot") == "de_groot"
        assert slugify("Test - Name") == "test_name"
    
    def test_multiple_underscores(self):
        """Test that multiple underscores are reduced to single."""
        assert slugify("Test___Multiple") == "test_multiple"
        assert slugify("a  b  c") == "a_b_c"
        assert slugify("x---y") == "x_y"
    
    def test_leading_trailing_underscores(self):
        """Test stripping of leading and trailing underscores."""
        assert slugify("_test_") == "test"
        assert slugify("__multiple__") == "multiple"
        assert slugify("-start") == "start"
        assert slugify("end-") == "end"
    
    def test_empty_and_none(self):
        """Test edge cases."""
        assert slugify("") == ""
        assert slugify("   ") == ""
        assert slugify("---") == ""
    
    def test_complex_names(self):
        """Test realistic complex names."""
        assert slugify("Jan-Willem de Groot") == "jan_willem_de_groot"
        assert slugify("François Müller") == "francois_muller"
        assert slugify("María José García") == "maria_jose_garcia"


class TestGenerateFilename:
    """Tests for generate_filename function."""
    
    def test_with_year(self):
        """Test filename generation with year."""
        filename = generate_filename(123, "portret", 1950, "jpg")
        # Format: 123_portret_1950_<uuid>.jpg
        assert filename.startswith("123_portret_1950_")
        assert filename.endswith(".jpg")
        assert len(filename.split("_")) == 4
    
    def test_without_year(self):
        """Test filename generation without year."""
        filename = generate_filename(123, "geboorteakte", extension="pdf")
        # Format: 123_geboorteakte_<uuid>.pdf
        assert filename.startswith("123_geboorteakte_")
        assert filename.endswith(".pdf")
        assert len(filename.split("_")) == 3  # id, type, uuid
    
    def test_year_zero(self):
        """Test that year 0 is omitted."""
        filename = generate_filename(123, "document", 0, "pdf")
        assert "_0_" not in filename
        assert filename.startswith("123_document_")
    
    def test_slugify_document_type(self):
        """Test that document type is slugified."""
        filename = generate_filename(123, "Familie Foto", 2020, "jpg")
        assert "familie_foto" in filename
        assert "Familie" not in filename
    
    def test_extension_normalization(self):
        """Test that extensions are normalized."""
        filename = generate_filename(123, "doc", extension=".pdf")
        assert filename.endswith(".pdf")
        assert not filename.endswith("..pdf")
    
    def test_uniqueness(self):
        """Test that generated filenames are unique."""
        filename1 = generate_filename(123, "portret", 1950, "jpg")
        filename2 = generate_filename(123, "portret", 1950, "jpg")
        # UUIDs should make them different
        assert filename1 != filename2


class TestGetPersonPath:
    """Tests for get_person_path function."""
    
    def test_basic_path(self):
        """Test basic person path generation."""
        path = get_person_path("/data", 123, "Jan", "Bakker")
        assert path == Path("/data/123/jan_bakker")
    
    def test_complex_name(self):
        """Test with complex names."""
        path = get_person_path("/data", 456, "Jan-Willem", "de Groot")
        assert path == Path("/data/456/jan_willem_de_groot")
    
    def test_accented_name(self):
        """Test with accented names."""
        path = get_person_path("/data", 789, "François", "Müller")
        assert path == Path("/data/789/francois_muller")


class TestGetFamilyPath:
    """Tests for get_family_path function."""
    
    def test_basic_family_path(self):
        """Test basic family path generation."""
        path = get_family_path(
            "/data", 
            123, "Jan", "Bakker",
            456, "Marie", "Jansen"
        )
        assert path == Path("/data/123_456/jan_bakker_marie_jansen")
    
    def test_complex_family_names(self):
        """Test with complex names."""
        path = get_family_path(
            "/data",
            111, "Jan-Willem", "de Groot",
            222, "Anne-Marie", "van der Berg"
        )
        assert path == Path("/data/111_222/jan_willem_de_groot_anne_marie_van_der_berg")


class TestEnsureDirectoryExists:
    """Tests for ensure_directory_exists function."""
    
    def test_create_directory(self, tmp_path):
        """Test directory creation."""
        test_dir = tmp_path / "test" / "nested" / "path"
        ensure_directory_exists(test_dir)
        assert test_dir.exists()
        assert test_dir.is_dir()
    
    def test_existing_directory(self, tmp_path):
        """Test with existing directory (should not fail)."""
        test_dir = tmp_path / "existing"
        test_dir.mkdir()
        ensure_directory_exists(test_dir)  # Should not raise
        assert test_dir.exists()


class TestGetStorageBasePath:
    """Tests for get_storage_base_path function."""
    
    def test_development_environment(self):
        """Test development path."""
        path = get_storage_base_path("development")
        # Should return development path
        assert "/home/frans/Documenten/Dev/Familiez/BESTANDEN" in path or "STORAGE_BASE_PATH" in str(path)
    
    def test_production_environment(self):
        """Test production path."""
        path = get_storage_base_path("production")
        # Should return production path or environment variable
        assert path  # Just check it returns something


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
