"""
Dimension table loading utilities.

Handles loading songs and users reference data from S3 or local storage.
"""

import logging

logger = logging.getLogger(__name__)


def load_songs(spark, path):
    """
    Load and cache the songs dimension CSV.

    Expected columns: track_id, track_name, artists, popularity, duration_ms, track_genre.

    Args:
        spark: SparkSession object.
        path: Path to songs.csv (local or S3).

    Returns:
        Spark DataFrame with songs data.
    """
    logger.info(f"Loading songs from {path}")

    df = spark.read.csv(path, header=True, inferSchema=True).cache()
    logger.info(f"Loaded {df.count()} songs")

    return df


def load_users(spark, path):
    """
    Load and cache the users dimension CSV.

    Expected columns: user_id, user_name, user_country, user_age, subscription_type.

    Args:
        spark: SparkSession object.
        path: Path to users.csv (local or S3).

    Returns:
        Spark DataFrame with users data.
    """
    logger.info(f"Loading users from {path}")

    df = spark.read.csv(path, header=True, inferSchema=True).cache()
    logger.info(f"Loaded {df.count()} users")

    return df


def load_dimensions(spark, songs_path, users_path):
    """
    Load both dimension tables and return them as a tuple.

    Args:
        spark: SparkSession object.
        songs_path: Path to songs.csv.
        users_path: Path to users.csv.

    Returns:
        Tuple of (songs_df, users_df).
    """
    songs = load_songs(spark, songs_path)
    users = load_users(spark, users_path)

    return songs, users
