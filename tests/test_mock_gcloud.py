"""
Test module to verify the mock gcloud functionality using pytest.
Updated to match the simplified mock gcloud behavior.
"""

import json
import os
import subprocess
import tempfile
import time

import pytest  # noqa: F401


def create_test_job_config(script_text=None):
    """Helper function to create a test job config"""
    if script_text is None:
        script_text = "echo 'Hello from mock batch job!'; echo 'Working directory:'"

    return {
        "taskGroups": [
            {
                "taskSpec": {
                    "runnables": [{"script": {"text": script_text}}],
                    "environment": {"variables": {"TEST_VAR": "test_value"}},
                    "volumes": [
                        {
                            "gcs": {"remotePath": "gs://test-bucket/input/data"},
                            "mountPath": "/mnt/disks/input",
                            "deviceName": "input",
                        }
                    ],
                }
            }
        ],
        "allocationPolicy": {
            "instances": [
                {
                    "policy": {
                        "disks": [
                            {
                                "gcs": {"remotePath": "gs://test-bucket/output"},
                                "mountPath": "/mnt/disks/output",
                                "deviceName": "output",
                            }
                        ]
                    }
                }
            ]
        },
    }


def submit_test_job(mock_gcloud_path, job_name="test-job", script_text=None):
    """Helper function to submit a test job and return job info"""
    test_config = create_test_job_config(script_text)

    # Write config to temporary file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(test_config, f, indent=2)
        config_file = f.name

    try:
        # Test job submission - mock now doesn't return JSON, just submits
        result = subprocess.run(
            [
                str(mock_gcloud_path),
                "batch",
                "jobs",
                "submit",
                job_name,
                "--config",
                config_file,
                "--project",
                "test-project",
                "--location",
                "us-central1",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, f"Job submission failed: {result.stderr}"

        # Mock now just creates the job directory with job_name
        return job_name, config_file

    except Exception:
        os.unlink(config_file)
        raise


# @pytest.mark.forked
def test_mock_gcloud_submission(mock_gcloud_path, mock_jobs_dir):
    """Test the mock gcloud batch job submission"""
    job_name, config_file = submit_test_job(
        mock_gcloud_path,
        "test_mock_gcloud_submission",
    )

    try:
        # Verify job directory was created
        job_dir = mock_jobs_dir / job_name
        assert job_dir.exists(), "Job directory not created"

        # Verify job.jid file exists
        # jid_file = job_dir / "job.jid"
        # Framework should create this, not gcloud mock
        # assert jid_file.exists(), "Job JID file not created"

        # Verify wrapped script was created
        script_file = job_dir / "job.runnable"
        assert script_file.exists(), "Job script not created"

        pid_file = job_dir / "job.pid"
        assert pid_file.exists(), "Job PID file not created"

    finally:
        os.unlink(config_file)


#
def test_mock_gcloud_describe_not_running(mock_gcloud_path):
    """Test the mock gcloud batch job describe"""
    job_name, config_file = submit_test_job(
        mock_gcloud_path,
        "test_mock_gcloud_describe_not_running",
    )

    try:
        # Test job describe
        result = subprocess.run(
            [
                str(mock_gcloud_path),
                "batch",
                "jobs",
                "describe",
                job_name,
                "--project",
                "test-project",
                "--location",
                "us-central1",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "state: UNKNOWN" in result.stdout
    finally:
        os.unlink(config_file)


@pytest.mark.forked
def test_mock_gcloud_describe_running(mock_gcloud_path):
    """Test the mock gcloud batch job describe"""
    job_name, config_file = submit_test_job(
        mock_gcloud_path,
        "test_mock_gcloud_describe_running",
        "sleep 10",
    )

    try:
        # Test job describe
        result = subprocess.run(
            [
                str(mock_gcloud_path),
                "batch",
                "jobs",
                "describe",
                job_name,
                "--project",
                "test-project",
                "--location",
                "us-central1",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "state: RUNNING" in result.stdout
    finally:
        os.unlink(config_file)


# @pytest.mark.forked
def test_mock_gcloud_script_content(mock_gcloud_path, mock_jobs_dir):
    """Test that the generated script contains expected content"""
    job_name, config_file = submit_test_job(
        mock_gcloud_path,
        "test_mock_gcloud_script_content",
    )

    try:
        # Check job script content
        job_dir = mock_jobs_dir / job_name
        script_file = job_dir / "job.runnable"

        # Wait for script to be created
        for i in range(10):
            if script_file.exists():
                break
            time.sleep(0.1)

        assert script_file.exists(), "Job script not found"

        # Verify script contains expected content
        script_content = script_file.read_text()
        assert "Hello from mock batch job!" in script_content
        assert "#!/bin/bash" in script_content
        assert "set -e" in script_content

    finally:
        os.unlink(config_file)


# @pytest.mark.forked
def test_mock_gcloud_delete(mock_gcloud_path, mock_jobs_dir):
    """Test the mock gcloud batch job deletion"""
    job_name, config_file = submit_test_job(
        mock_gcloud_path,
        "test_mock_gcloud_delete",
        "sleep 100",
    )

    try:
        result = subprocess.run(
            [
                str(mock_gcloud_path),
                "batch",
                "jobs",
                "describe",
                job_name,
                "--project",
                "test-project",
                "--location",
                "us-central1",
            ],
            capture_output=True,
            text=True,
        )
        assert "state: RUNNING" in result.stdout

        # Test job deletion
        subprocess.run(
            [
                str(mock_gcloud_path),
                "batch",
                "jobs",
                "delete",
                job_name,
                "--project",
                "test-project",
                "--quiet",
            ],
            capture_output=True,
            text=True,
        )

        result = subprocess.run(
            [
                str(mock_gcloud_path),
                "batch",
                "jobs",
                "describe",
                job_name,
                "--project",
                "test-project",
                "--location",
                "us-central1",
            ],
            capture_output=True,
            text=True,
        )
        assert "state: UNKNOWN" in result.stdout

    finally:
        os.unlink(config_file)
