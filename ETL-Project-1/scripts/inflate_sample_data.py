from __future__ import annotations

import csv
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

TARGET_MB = {
    "songs.csv": 2,
    "users.csv": 2,
    "streams1.csv": 5,
    "streams2.csv": 5,
    "streams3.csv": 5,
}

# Sample data templates for generating fresh synthetic data
SAMPLE_TRACK_NAMES = [
    "Blinding Lights", "Shape of You", "Someone Like You", "Rolling in the Deep",
    "Uptown Funk", "Bohemian Rhapsody", "Stairway to Heaven", "Hotel California",
    "Imagine", "Let It Be", "Like a Rolling Stone", "All Along the Watchtower",
    "Sweet Home Chicago", "The Sound of Silence", "Black", "Smells Like Teen Spirit"
]

SAMPLE_ARTISTS = [
    "The Weeknd", "Ed Sheeran", "Adele", "Post Malone", "Drake",
    "Billie Eilish", "The Weeknd", "Taylor Swift", "Ariana Grande",
    "Dua Lipa", "Justin Bieber", "Eminem", "Mariah Carey", "Rihanna",
    "Lady Gaga", "Katy Perry", "Beyoncé", "Bruno Mars"
]

SAMPLE_USER_NAMES = [
    "Alex", "Jordan", "Morgan", "Casey", "Riley", "Devon", "Avery",
    "Quinn", "Sage", "Taylor", "Cameron", "Skyler", "River", "Sydney"
]

SAMPLE_COUNTRIES = [
    "US", "UK", "CA", "AU", "DE", "FR", "IT", "ES", "BR", "IN",
    "JP", "KR", "MX", "SE", "NL", "CH", "BE", "AT", "NO", "DK"
]


def target_bytes(filename):
    """Convert configured target size from MB to bytes for a file."""
    # Extract base filename without timestamp
    if "songs" in filename:
        size_mb = TARGET_MB.get("songs.csv", 2)
    elif "users" in filename:
        size_mb = TARGET_MB.get("users.csv", 2)
    elif "streams1" in filename:
        size_mb = TARGET_MB.get("streams1.csv", 5)
    elif "streams2" in filename:
        size_mb = TARGET_MB.get("streams2.csv", 5)
    elif "streams3" in filename:
        size_mb = TARGET_MB.get("streams3.csv", 5)
    else:
        size_mb = TARGET_MB.get(filename, 2)
    return size_mb * 1024 * 1024


def generate_song_row(song_id):
    """Generate a fresh synthetic song record."""
    return {
        "id": str(song_id),
        "track_id": f"track_{song_id:06d}",
        "track_name": f"{random.choice(SAMPLE_TRACK_NAMES)} {random.randint(1, 100)}",
        "artists": random.choice(SAMPLE_ARTISTS),
        "popularity": str(random.randint(20, 100)),
        "ingest_ts": (datetime.now() - timedelta(days=random.randint(0, 180))).isoformat(),
    }


def generate_user_row(user_id):
    """Generate a fresh synthetic user record."""
    created_date = datetime(2023, 1, 1) + \
        timedelta(days=random.randint(0, 730))
    return {
        "user_id": str(user_id),
        "user_name": f"{random.choice(SAMPLE_USER_NAMES)}_{random.randint(1000, 9999)}",
        "user_age": str(random.randint(13, 85)),
        "user_country": random.choice(SAMPLE_COUNTRIES),
        "created_at": created_date.strftime("%Y-%m-%d"),
        "ingest_ts": (datetime.now() - timedelta(days=random.randint(0, 180))).isoformat(),
    }


def generate_stream_row(user_ids, track_ids):
    """Generate a fresh synthetic stream record."""
    listen_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))
    return {
        "stream_event_id": f"stream_{int(time.time() * 1000000) + random.randint(0, 999999)}",
        "user_id": random.choice(user_ids),
        "track_id": random.choice(track_ids),
        "listen_time": listen_date.strftime("%Y-%m-%d %H:%M:%S"),
        "ingest_ts": (datetime.now() - timedelta(days=random.randint(0, 180))).isoformat(),
    }


def generate_songs_file(path):
    """Generate a fresh songs.csv file with synthetic data."""
    target_size = target_bytes(path.name)
    header = ["id", "track_id", "track_name",
              "artists", "popularity", "ingest_ts"]

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    song_id = 1

    with tmp_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()

        while tmp_path.stat().st_size < target_size:
            writer.writerow(generate_song_row(song_id))
            song_id += 1
            if song_id % 500 == 0:
                handle.flush()

        handle.flush()

    tmp_path.replace(path)
    print(f"{path.name}: generated {path.stat().st_size} bytes (target {target_size})")
    return song_id


def generate_users_file(path):
    """Generate a fresh users.csv file with synthetic data."""
    target_size = target_bytes(path.name)
    header = ["user_id", "user_name", "user_age",
              "user_country", "created_at", "ingest_ts"]

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    user_id = 1

    with tmp_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()

        while tmp_path.stat().st_size < target_size:
            writer.writerow(generate_user_row(user_id))
            user_id += 1
            if user_id % 500 == 0:
                handle.flush()

        handle.flush()

    tmp_path.replace(path)
    print(f"{path.name}: generated {path.stat().st_size} bytes (target {target_size})")
    return user_id


def generate_streams_file(path, user_ids, track_ids):
    """Generate a fresh streams.csv file with synthetic data."""
    target_size = target_bytes(path.name)
    header = ["stream_event_id", "user_id",
              "track_id", "listen_time", "ingest_ts"]

    tmp_path = path.with_suffix(path.suffix + ".tmp")

    with tmp_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()

        while tmp_path.stat().st_size < target_size:
            writer.writerow(generate_stream_row(user_ids, track_ids))
            if tmp_path.stat().st_size % 5000000 == 0:
                handle.flush()

        handle.flush()

    tmp_path.replace(path)
    print(f"{path.name}: generated {path.stat().st_size} bytes (target {target_size})")


def main(force_regenerate=False):
    """
    Generate fresh synthetic CSV files under sample_data_synthetic directory.

    Args:
        force_regenerate: If True, always regenerate files (ignored, always regenerates).

    Returns:
        None.
    """
    seed = int(time.time()) % 100000
    random.seed(seed)
    print(f"Generating fresh synthetic data with seed: {seed}")

    root = Path("sample_data_synthetic")
    root.mkdir(exist_ok=True)

    # Generate timestamp suffix for unique filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Generate songs first
    songs_path = root / f"songs_{timestamp}.csv"
    num_songs = generate_songs_file(songs_path)
    track_ids = [f"track_{i:06d}" for i in range(1, num_songs)]

    # Generate users
    users_path = root / f"users_{timestamp}.csv"
    num_users = generate_users_file(users_path)
    user_ids = [str(i) for i in range(1, num_users)]

    # Generate streams with references to generated songs and users
    for stream_num in [1, 2, 3]:
        stream_path = root / f"streams{stream_num}_{timestamp}.csv"
        generate_streams_file(stream_path, user_ids, track_ids)


if __name__ == "__main__":
    import sys
    force_regen = "--regenerate" in sys.argv or "-r" in sys.argv
    main(force_regenerate=force_regen)
