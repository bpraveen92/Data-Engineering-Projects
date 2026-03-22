"""
Download NYC TLC Yellow Taxi Trip Records from the public TLC cloudfront URL.

Usage:
    python scripts/download_data.py --months 2024-01 2024-02 2024-03

Files are saved to:
    data/raw/yellow_tripdata_{YYYY-MM}.parquet

No authentication or API keys required — the TLC dataset is publicly available.
The cloudfront URL pattern is stable and has been in use since ~2022.
"""

import argparse
import sys
from pathlib import Path

import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw"


def download_month(month):
    """
    Download the Parquet file for one YYYY-MM month string.

    Parameters
    ----------
    month : str
        Month in YYYY-MM format, e.g. '2024-01'.

    Returns
    -------
    Path
        Local path where the file was saved.
    """
    url = f"{BASE_URL}/yellow_tripdata_{month}.parquet"
    output_path = OUTPUT_DIR / f"yellow_tripdata_{month}.parquet"

    if output_path.exists():
        print(f"[skip]     {output_path.name} already exists")
        return output_path

    print(f"[download] {url}")
    response = requests.get(url, stream=True, timeout=120)

    if response.status_code == 404:
        print(f"[error]    404 — data for {month} not yet published", file=sys.stderr)
        sys.exit(1)

    response.raise_for_status()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"[saved]    {output_path.name}  ({size_mb:.1f} MB)")
    return output_path


def main():
    parser = argparse.ArgumentParser(
        description="Download NYC TLC Yellow Taxi Parquet files"
    )
    parser.add_argument(
        "--months",
        nargs="+",
        required=True,
        metavar="YYYY-MM",
        help="One or more months to download, e.g. 2024-01 2024-02 2024-03",
    )
    args = parser.parse_args()

    for month in args.months:
        download_month(month)

    print(f"\nDone. Files are in {OUTPUT_DIR}/")
    print("Next step:  make load MONTH=<YYYY-MM>  (repeat for each month)")


if __name__ == "__main__":
    main()
