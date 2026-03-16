from __future__ import annotations

from pathlib import Path

SUPPORTED_DATASETS = ("songs", "users", "streams")


# Infer dataset name from the file name prefix.
def dataset_from_key(key):
    """
    Infer dataset name from an S3 object key or file path.

    Args:
        key: Object key or file path string.
            Example: "Project-1/raw/songs.csv" or "users_backup_20250228.csv"

    Returns:
        Dataset name when matched, otherwise None.
            Example: "songs" for "Project-1/raw/songs.csv"
            Example: "users" for "users_backup_20250228.csv"
            Example: None for "metadata.txt"
    """
    base_name = Path(key).name.lower()
    for dataset in SUPPORTED_DATASETS:
        if base_name.startswith(dataset):
            return dataset
    return None


# Return unique datasets in the order they appear in file keys.
def unique_datasets(file_keys):
    """
    Build ordered unique dataset names from incoming file keys.

    Args:
        file_keys: List of raw object keys to inspect.
            Example: [
                "Project-1/raw/songs.csv",
                "Project-1/raw/users.csv",
                "Project-1/raw/streams_20250228.csv",
                "Project-1/raw/songs_backup.csv"
            ]

    Returns:
        List of unique dataset names preserving first-seen order.
            Example: ["songs", "users", "streams"]
    """
    found = []
    for key in file_keys:
        dataset = dataset_from_key(key)
        if dataset and dataset not in found:
            found.append(dataset)
    return found


# Group input file keys by dataset.
def group_keys_by_dataset(file_keys):
    """
    Group raw file keys by dataset name.

    Args:
        file_keys: List of raw object keys.
            Example: [
                "Project-1/raw/songs.csv",
                "Project-1/raw/users.csv",
                "Project-1/raw/streams_20250228.csv",
                "Project-1/raw/songs_backup.csv"
            ]

    Returns:
        Dictionary where each key is a dataset and value is matching file keys.
            Example: {
                "songs": [
                    "Project-1/raw/songs.csv",
                    "Project-1/raw/songs_backup.csv"
                ],
                "users": ["Project-1/raw/users.csv"],
                "streams": ["Project-1/raw/streams_20250228.csv"]
            }
    """
    grouped = {}
    for key in file_keys:
        dataset = dataset_from_key(key)
        if dataset is None:
            continue
        grouped.setdefault(dataset, []).append(key)
    return grouped
