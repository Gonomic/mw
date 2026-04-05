import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from fastapi.testclient import TestClient

from main import app, format_result, fetch_releases


# Create test client
client = TestClient(app)


# ==================== Tests for format_result function ====================

class TestFormatResult:
    """Test suite for the format_result utility function."""

    def test_format_result_empty_list(self):
        """Test format_result returns correct structure for empty results."""
        result = format_result([])
        assert result == [{"numberOfRecords": 0}]
        assert len(result) == 1

    def test_format_result_single_record(self):
        """Test format_result with a single database record."""
        # Mock a database row object
        mock_row = Mock()
        mock_row._asdict.return_value = {"id": 1, "name": "John"}
        
        result = format_result([mock_row])
        
        assert len(result) == 2
        assert result[0] == {"numberOfRecords": 1}
        assert result[1] == {"id": 1, "name": "John"}

    def test_format_result_multiple_records(self):
        """Test format_result with multiple database records."""
        mock_row1 = Mock()
        mock_row1._asdict.return_value = {"id": 1, "name": "John"}
        
        mock_row2 = Mock()
        mock_row2._asdict.return_value = {"id": 2, "name": "Jane"}
        
        mock_row3 = Mock()
        mock_row3._asdict.return_value = {"id": 3, "name": "Bob"}
        
        result = format_result([mock_row1, mock_row2, mock_row3])
        
        assert len(result) == 4
        assert result[0] == {"numberOfRecords": 3}
        assert result[1] == {"id": 1, "name": "John"}
        assert result[2] == {"id": 2, "name": "Jane"}
        assert result[3] == {"id": 3, "name": "Bob"}


# ==================== Tests for API Endpoints ====================

class TestRootEndpoint:
    """Test suite for the root endpoint."""

    def test_read_root(self):
        """Test root endpoint returns correct welcome message."""
        response = client.get("/")
        
        assert response.status_code == 200
        assert "Hello visitor" in response.json()
        assert response.json()["Hello visitor"] == "The Familiez Fastapi api lives!"


class TestPingAPIEndpoint:
    """Test suite for the ping API endpoint."""

    def test_ping_api_with_valid_timestamp(self):
        """Test ping API endpoint returns both frontend and middleware timestamps."""
        test_time = datetime.now().isoformat()
        
        response = client.get(f"/pingAPI?timestampFE={test_time}")
        
        assert response.status_code == 200
        result = response.json()
        assert len(result) == 1
        assert "FE request time" in result[0]
        assert "MW request time" in result[0]

    def test_ping_api_missing_timestamp(self):
        """Test ping API endpoint without timestamp parameter."""
        response = client.get("/pingAPI")
        
        # Should fail validation
        assert response.status_code == 422  # Unprocessable Entity


class TestPingDBEndpoint:
    """Test suite for the ping database endpoint."""

    @patch('main.engine')
    def test_ping_db_success(self, mock_engine):
        """Test successful database ping."""
        # Setup mock
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        
        mock_result = Mock()
        mock_result._asdict.return_value = {
            "datetimeFErequest": datetime.now(),
            "timestampMWrequest": datetime.now()
        }
        
        mock_results_proxy = Mock()
        mock_results_proxy.fetchall.return_value = [mock_result]
        mock_connection.execute.return_value = mock_results_proxy
        
        test_time = datetime.now().isoformat()
        response = client.get(f"/pingDB?timestampFE={test_time}")
        
        assert response.status_code == 200
        result = response.json()
        assert isinstance(result, list)
        assert len(result) > 0

    @patch('main.engine')
    def test_ping_db_connection_error(self, mock_engine):
        """Test database ping with connection error."""
        # Setup mock to raise exception
        mock_engine.connect.return_value.__enter__.side_effect = Exception("Connection failed")
        
        test_time = datetime.now().isoformat()
        response = client.get(f"/pingDB?timestampFE={test_time}")
        
        assert response.status_code == 500
        assert "Database connection failed" in response.json()["detail"]


class TestGetPersonsLikeEndpoint:
    """Test suite for the GetPersonsLike endpoint."""

    @patch('main.engine')
    def test_get_persons_like_with_results(self, mock_engine):
        """Test GetPersonsLike returns formatted results."""
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        
        mock_row = Mock()
        mock_row._asdict.return_value = {"id": 1, "firstName": "John", "lastName": "Doe"}
        
        mock_results_proxy = Mock()
        mock_results_proxy.fetchall.return_value = [mock_row]
        mock_connection.execute.return_value = mock_results_proxy
        
        response = client.get("/GetPersonsLike?stringToSearchFor=John")
        
        assert response.status_code == 200
        result = response.json()
        assert result[0]["numberOfRecords"] == 1
        assert result[1]["firstName"] == "John"

    @patch('main.engine')
    def test_get_persons_like_no_results(self, mock_engine):
        """Test GetPersonsLike with no matching results."""
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        
        mock_results_proxy = Mock()
        mock_results_proxy.fetchall.return_value = []
        mock_connection.execute.return_value = mock_results_proxy
        
        response = client.get("/GetPersonsLike?stringToSearchFor=NonExistent")
        
        assert response.status_code == 200
        result = response.json()
        assert result[0]["numberOfRecords"] == 0

    def test_get_persons_like_missing_parameter(self):
        """Test GetPersonsLike without required parameter."""
        response = client.get("/GetPersonsLike")
        
        assert response.status_code == 422  # Unprocessable Entity

    @patch('main.engine')
    def test_get_persons_like_query_error(self, mock_engine):
        """Test GetPersonsLike with database query error."""
        mock_engine.connect.return_value.__enter__.side_effect = Exception("Query error")
        
        response = client.get("/GetPersonsLike?stringToSearchFor=test")
        
        assert response.status_code == 500
        assert "Query failed" in response.json()["detail"]


class TestGetSiblingsEndpoint:
    """Test suite for the GetSiblings endpoint."""

    @patch('main.engine')
    def test_get_siblings_with_results(self, mock_engine):
        """Test GetSiblings returns formatted results."""
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        
        mock_row1 = Mock()
        mock_row1._asdict.return_value = {"id": 2, "name": "Jane"}
        
        mock_row2 = Mock()
        mock_row2._asdict.return_value = {"id": 3, "name": "Bob"}
        
        mock_results_proxy = Mock()
        mock_results_proxy.fetchall.return_value = [mock_row1, mock_row2]
        mock_connection.execute.return_value = mock_results_proxy
        
        response = client.get("/GetSiblings?parentID=1")
        
        assert response.status_code == 200
        result = response.json()
        assert result[0]["numberOfRecords"] == 2
        assert len(result) == 3  # numberOfRecords + 2 rows

    @patch('main.engine')
    def test_get_siblings_no_results(self, mock_engine):
        """Test GetSiblings with no siblings found."""
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        
        mock_results_proxy = Mock()
        mock_results_proxy.fetchall.return_value = []
        mock_connection.execute.return_value = mock_results_proxy
        
        response = client.get("/GetSiblings?parentID=999")
        
        assert response.status_code == 200
        result = response.json()
        assert result[0]["numberOfRecords"] == 0

    def test_get_siblings_missing_parameter(self):
        """Test GetSiblings without required parameter."""
        response = client.get("/GetSiblings")
        
        assert response.status_code == 422

    def test_get_siblings_invalid_parameter(self):
        """Test GetSiblings with invalid parentID parameter."""
        response = client.get("/GetSiblings?parentID=invalid")
        
        assert response.status_code == 422

    @patch('main.engine')
    def test_get_siblings_database_error(self, mock_engine):
        """Test GetSiblings with database error."""
        mock_engine.connect.return_value.__enter__.side_effect = Exception("DB error")
        
        response = client.get("/GetSiblings?parentID=1")
        
        assert response.status_code == 500
        assert "Query failed" in response.json()["detail"]


class TestGetFatherEndpoint:
    """Test suite for the GetFather endpoint."""

    @patch('main.engine')
    def test_get_father_with_result(self, mock_engine):
        """Test GetFather returns father information."""
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        
        mock_row = Mock()
        mock_row._asdict.return_value = {"id": 1, "name": "John Sr", "birthDate": "1950-01-01"}
        
        mock_results_proxy = Mock()
        mock_results_proxy.fetchall.return_value = [mock_row]
        mock_connection.execute.return_value = mock_results_proxy
        
        response = client.get("/GetFather?childID=5")
        
        assert response.status_code == 200
        result = response.json()
        assert result[0]["numberOfRecords"] == 1
        assert result[1]["name"] == "John Sr"

    @patch('main.engine')
    def test_get_father_not_found(self, mock_engine):
        """Test GetFather when father doesn't exist."""
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        
        mock_results_proxy = Mock()
        mock_results_proxy.fetchall.return_value = []
        mock_connection.execute.return_value = mock_results_proxy
        
        response = client.get("/GetFather?childID=999")
        
        assert response.status_code == 200
        result = response.json()
        assert result[0]["numberOfRecords"] == 0

    def test_get_father_missing_parameter(self):
        """Test GetFather without required parameter."""
        response = client.get("/GetFather")
        
        assert response.status_code == 422

    def test_get_father_invalid_parameter(self):
        """Test GetFather with invalid childID parameter."""
        response = client.get("/GetFather?childID=notanumber")
        
        assert response.status_code == 422

    @patch('main.engine')
    def test_get_father_database_error(self, mock_engine):
        """Test GetFather with database error."""
        mock_engine.connect.return_value.__enter__.side_effect = Exception("DB connection lost")
        
        response = client.get("/GetFather?childID=1")
        
        assert response.status_code == 500
        assert "Query failed" in response.json()["detail"]


class TestGetPersonDetailsEndpoint:
    """Test suite for the GetPersonDetails endpoint."""

    @patch('main.verify_sso_token')
    @patch('main.engine')
    def test_get_person_details_calls_v2_sproc(self, mock_engine, mock_verify_sso_token):
        """Test GetPersonDetails uses GetPersonDetails_v2 stored procedure."""
        mock_verify_sso_token.return_value = {"sub": "test-user"}
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        mock_row = Mock()
        mock_row._asdict.return_value = {
            "PersonID": 5,
            "PersonGivvenName": "Jan",
            "PersonFamilyName": "Jansen",
            "PersonDateOfBirthStatus": "Exact",
        }

        mock_results_proxy = Mock()
        mock_results_proxy.fetchall.return_value = [mock_row]
        mock_connection.execute.return_value = mock_results_proxy

        response = client.get(
            "/GetPersonDetails?personID=5",
            headers={"Authorization": "Bearer valid-test-token"},
        )

        assert response.status_code == 200
        result = response.json()
        assert result[0]["numberOfRecords"] == 1
        assert result[1]["PersonID"] == 5

        call_args = mock_connection.execute.call_args
        assert "GetPersonDetails_v2" in str(call_args[0][0])
        assert call_args[0][1]["personId"] == 5

    @patch('main.verify_sso_token')
    @patch('main.engine')
    def test_get_person_details_database_error(self, mock_engine, mock_verify_sso_token):
        """Test GetPersonDetails with database error."""
        mock_verify_sso_token.return_value = {"sub": "test-user"}
        mock_engine.connect.return_value.__enter__.side_effect = Exception("DB connection lost")

        response = client.get(
            "/GetPersonDetails?personID=5",
            headers={"Authorization": "Bearer valid-test-token"},
        )

        assert response.status_code == 500
        assert "Query failed" in response.json()["detail"]


class TestGetPartnersEndpoint:
    """Test suite for the GetPartners endpoint."""

    @patch('main.verify_sso_token')
    @patch('main.engine')
    def test_get_partners_calls_sproc(self, mock_engine, mock_verify_sso_token):
        """Test GetPartners uses GetPartnerForPerson stored procedure."""
        mock_verify_sso_token.return_value = {"sub": "test-user"}

        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        mock_row = Mock()
        mock_row._asdict.return_value = {
            "PersonID": 7,
            "PersonGivvenName": "Piet",
            "PersonFamilyName": "Pieters",
        }

        mock_results_proxy = Mock()
        mock_results_proxy.fetchall.return_value = [mock_row]
        mock_connection.execute.return_value = mock_results_proxy

        response = client.get(
            "/GetPartners?personID=5",
            headers={"Authorization": "Bearer valid-test-token"},
        )

        assert response.status_code == 200
        result = response.json()
        assert result[0]["numberOfRecords"] == 1
        assert result[1]["PersonID"] == 7

        call_args = mock_connection.execute.call_args
        assert "GetPartnerForPerson" in str(call_args[0][0])
        assert call_args[0][1]["personId"] == 5

    @patch('main.verify_sso_token')
    @patch('main.engine')
    def test_get_partners_database_error(self, mock_engine, mock_verify_sso_token):
        """Test GetPartners with database error."""
        mock_verify_sso_token.return_value = {"sub": "test-user"}
        mock_engine.connect.return_value.__enter__.side_effect = Exception("DB connection lost")

        response = client.get(
            "/GetPartners?personID=5",
            headers={"Authorization": "Bearer valid-test-token"},
        )

        assert response.status_code == 500
        assert "Query failed" in response.json()["detail"]


class TestFetchReleases:
    """Test suite for fetch_releases helper."""

    @patch('main.engine')
    def test_fetch_releases_uses_sproc_and_groups_changes(self, mock_engine):
        """fetch_releases should call GetReleasesByComponent and preserve grouped response shape."""
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        row1 = Mock()
        row1.ReleaseID = 10
        row1.ReleaseNumber = '0.9.8'
        row1.ReleaseDate = '2026-04-05 10:00:00'
        row1.Description = 'desc'
        row1.ChangeID = 100
        row1.ChangeDescription = 'change 1'
        row1.ChangeType = 'feature'

        row2 = Mock()
        row2.ReleaseID = 10
        row2.ReleaseNumber = '0.9.8'
        row2.ReleaseDate = '2026-04-05 10:00:00'
        row2.Description = 'desc'
        row2.ChangeID = 101
        row2.ChangeDescription = 'change 2'
        row2.ChangeType = 'refactor'

        mock_connection.execute.return_value.fetchall.return_value = [row1, row2]

        result = fetch_releases('mw')

        assert len(result) == 1
        assert result[0]['ReleaseID'] == 10
        assert result[0]['Component'] == 'mw'
        assert len(result[0]['Changes']) == 2

        call_args = mock_connection.execute.call_args
        assert 'GetReleasesByComponent' in str(call_args[0][0])
        assert call_args[0][1]['componentIn'] == 'mw'

    def test_fetch_releases_invalid_component(self):
        """fetch_releases should reject unknown components."""
        with pytest.raises(Exception):
            fetch_releases('invalid-component')


class TestFileReadEndpoints:
    """Test suite for file read endpoints migrated to sprocs."""

    @patch('main.verify_sso_token')
    @patch('main.engine')
    def test_download_file_calls_get_file_meta(self, mock_engine, mock_verify_sso_token):
        """Download endpoint should fetch metadata using GetFileMeta sproc."""
        mock_verify_sso_token.return_value = {'sub': 'test-user'}
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        mock_result = Mock()
        mock_result.FilePath = 'missing/path/file.pdf'
        mock_result.FileName = 'file.pdf'
        mock_result.OriginalFileName = 'file.pdf'
        mock_result.MimeType = 'application/pdf'
        mock_connection.execute.return_value.fetchone.return_value = mock_result

        response = client.get(
            '/api/files/123',
            headers={'Authorization': 'Bearer valid-test-token'},
        )

        assert response.status_code == 404
        call_args = mock_connection.execute.call_args
        assert 'GetFileMeta' in str(call_args[0][0])
        assert call_args[0][1]['file_id'] == 123

    @patch('main.verify_sso_token')
    @patch('main.engine')
    def test_person_files_calls_get_person_files(self, mock_engine, mock_verify_sso_token):
        """Person files endpoint should use GetPersonFiles sproc."""
        mock_verify_sso_token.return_value = {'sub': 'test-user'}
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        mock_row = Mock()
        mock_row.FileID = 5
        mock_row.FileName = 'x.pdf'
        mock_row.OriginalFileName = 'x.pdf'
        mock_row.DocumentType = 'akte'
        mock_row.Year = 1999
        mock_row.FileSize = 12
        mock_row.MimeType = 'application/pdf'
        mock_row.CreatedAt = datetime(2026, 4, 5, 11, 0, 0)
        mock_row.UploadedBy = 'user'

        mock_connection.execute.return_value.fetchall.return_value = [mock_row]

        response = client.get(
            '/api/person/10/files',
            headers={'Authorization': 'Bearer valid-test-token'},
        )

        assert response.status_code == 200
        payload = response.json()
        assert len(payload) == 1
        assert payload[0]['file_id'] == 5

        call_args = mock_connection.execute.call_args
        assert 'GetPersonFiles' in str(call_args[0][0])
        assert call_args[0][1]['person_id'] == 10

    @patch('main.verify_sso_token')
    @patch('main.engine')
    def test_family_files_calls_get_family_files(self, mock_engine, mock_verify_sso_token):
        """Family files endpoint should use GetFamilyFiles sproc."""
        mock_verify_sso_token.return_value = {'sub': 'test-user'}
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.execute.return_value.fetchall.return_value = []

        response = client.get(
            '/api/family/2/3/files',
            headers={'Authorization': 'Bearer valid-test-token'},
        )

        assert response.status_code == 200
        assert response.json() == []

        call_args = mock_connection.execute.call_args
        assert 'GetFamilyFiles' in str(call_args[0][0])
        assert call_args[0][1]['father_id'] == 2
        assert call_args[0][1]['mother_id'] == 3
