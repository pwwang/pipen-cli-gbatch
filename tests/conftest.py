import os
import pytest
from pathlib import Path


@pytest.fixture(scope="session", autouse=True)
def setup_mock_gcloud():
    """Setup mock gcloud script to be executable for all tests"""
    mock_dir = Path(__file__).parent / "mock"
    gcloud_script = mock_dir / "gcloud"

    # Make the mock gcloud script executable
    if gcloud_script.exists():
        os.chmod(gcloud_script, 0o755)

    return str(mock_dir)


@pytest.fixture
def mock_gcloud_path(setup_mock_gcloud):
    """Provide path to mock gcloud script"""
    return Path(setup_mock_gcloud) / "gcloud"


@pytest.fixture
def mock_jobs_dir(setup_mock_gcloud):
    """Provide path to mock jobs directory"""
    jobs_dir = Path(setup_mock_gcloud) / "jobs"
    jobs_dir.mkdir(exist_ok=True)
    return jobs_dir


@pytest.fixture
def mock_mounts_dir(setup_mock_gcloud):
    """Provide path to mock mounts directory"""
    mounts_dir = Path(setup_mock_gcloud) / "mounts"
    mounts_dir.mkdir(exist_ok=True)
    return mounts_dir


MOCK_MOUNTS_DIR = Path(__file__).parent / "mock" / "mounts"
