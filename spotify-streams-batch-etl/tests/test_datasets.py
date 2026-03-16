from src.common.datasets import dataset_from_key, group_keys_by_dataset, unique_datasets


def test_dataset_from_key():
    """
    Validate dataset inference from representative file keys.

    Args:
        None.

    Returns:
        None.
    """
    assert dataset_from_key("inbound/songs.csv") == "songs"
    assert dataset_from_key("inbound/users_20260226.csv") == "users"
    assert dataset_from_key("inbound/streams3.csv") == "streams"
    assert dataset_from_key("inbound/other.csv") is None


def test_group_keys_by_dataset():
    """
    Validate grouping logic maps keys into expected dataset buckets.

    Args:
        None.

    Returns:
        None.
    """
    keys = [
        "inbound/songs.csv",
        "inbound/streams1.csv",
        "inbound/streams2.csv",
        "inbound/users.csv",
        "inbound/ignore.csv",
    ]
    grouped = group_keys_by_dataset(keys)
    assert set(grouped.keys()) == {"songs", "users", "streams"}
    assert len(grouped["streams"]) == 2


def test_unique_datasets_ordered():
    """
    Validate ordered uniqueness behavior for dataset extraction.

    Args:
        None.

    Returns:
        None.
    """
    keys = ["a/streams1.csv", "a/songs.csv", "a/streams2.csv", "a/users.csv"]
    assert unique_datasets(keys) == ["streams", "songs", "users"]
