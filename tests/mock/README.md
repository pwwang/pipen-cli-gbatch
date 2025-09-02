# Mock gcloud jobs and mounts directory structure

This directory contains mock state for the gcloud batch simulator.

## Structure

- `jobs/`: Directory containing job execution state and logs
- `mounts/`: Directory containing local mount points that simulate /mnt/disks/
- `gcloud`: The main mock executable

## Usage

The mock gcloud script will automatically create job directories and mount points as needed.
Jobs run locally and their status can be tracked through the standard gcloud commands.
