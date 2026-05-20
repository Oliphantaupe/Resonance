"""
Data acquisition script for the Resonance / bigdata-music project.

Usage:
    python scripts/download_data.py [--source all|kaggle|hf|countries]

Sources:
    kaggle    - charts.csv (~3.5 GB) via Kaggle API  [requires credentials]
    hf        - track_features.parquet (~4.3 GB) via huggingface_hub
    countries - countries.csv (built locally, no download needed)
"""

import argparse
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"


def download_hf():
    try:
        from huggingface_hub import hf_hub_download
    except ImportError:
        sys.exit("huggingface_hub not installed. Run: pip install huggingface_hub")

    target = RAW / "track_features.parquet"
    if target.exists():
        print(f"[hf] {target} already exists, skipping.")
        return

    print("[hf] Downloading spotify-huge-audio-features.parquet (~4.3 GB) ...")
    path = hf_hub_download(
        repo_id="GildasLeDrogoff/spotify-huge-track-analysis-dataset",
        filename="spotify-huge-audio-features.parquet",
        repo_type="dataset",
        local_dir=str(RAW),
        local_dir_use_symlinks=False,
    )
    if Path(path).resolve() != target.resolve():
        shutil.move(path, target)
    print(f"[hf] Saved → {target}")


def download_kaggle():
    # Check credentials — accept kaggle.json (classic) or access_token (new token format)
    kaggle_dir = Path.home() / ".kaggle"
    has_json  = (kaggle_dir / "kaggle.json").exists()
    has_token = (kaggle_dir / "access_token").exists()
    has_env   = bool(os.environ.get("KAGGLE_API_TOKEN") or
                     (os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY")))
    if not (has_json or has_token or has_env):
        print(
            "\n[kaggle] ERROR: No Kaggle credentials found.\n"
            "  Option A (new token):  save token to ~/.kaggle/access_token\n"
            "  Option B (classic):    place kaggle.json at ~/.kaggle/kaggle.json\n"
            "  Option C (env var):    export KAGGLE_API_TOKEN=<token>\n"
            "  Then run: python scripts/download_data.py --source kaggle\n"
        )
        sys.exit(1)

    try:
        import kaggle  # noqa: F401 — triggers credential load
    except ImportError:
        sys.exit("[kaggle] kaggle package not installed. Run: pip install kaggle")

    target = RAW / "charts.csv"
    if target.exists():
        print(f"[kaggle] {target} already exists, skipping.")
        return

    print("[kaggle] Downloading dhruvildave/spotify-charts (~3.5 GB) ...")
    import shutil
    import subprocess
    env = os.environ.copy()
    if has_token and not has_env:
        env["KAGGLE_API_TOKEN"] = (kaggle_dir / "access_token").read_text().strip()
    kaggle_bin = shutil.which("kaggle") or str(Path.home() / ".local" / "bin" / "kaggle")
    result = subprocess.run(
        [
            kaggle_bin, "datasets", "download",
            "-d", "dhruvildave/spotify-charts",
            "--unzip",
            "-p", str(RAW),
        ],
        check=True,
        env=env,
    )
    # The unzipped file is named 'charts.csv'
    downloaded = RAW / "charts.csv"
    if not downloaded.exists():
        # Sometimes the filename differs — find the CSV
        candidates = list(RAW.glob("*.csv"))
        charts_candidates = [f for f in candidates if "chart" in f.name.lower()]
        if charts_candidates:
            charts_candidates[0].rename(downloaded)
        else:
            print("[kaggle] Warning: could not locate downloaded CSV. Files in raw/:")
            for f in RAW.iterdir():
                print(f"  {f.name}")
            return
    print(f"[kaggle] Saved → {downloaded}")


def verify():
    """Print a quick size report for all raw files."""
    print("\n--- Raw data status ---")
    files = {
        "charts.csv": "~3.48 GB expected",
        "track_features.parquet": "~4.30 GB expected",
        "countries.csv": "<10 KB expected",
    }
    for name, note in files.items():
        p = RAW / name
        if p.exists():
            size_mb = p.stat().st_size / (1024 ** 2)
            print(f"  OK  {name:35s}  {size_mb:>8.1f} MB   ({note})")
        else:
            print(f"  !!  {name:35s}  MISSING       ({note})")
    print()


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", default="all",
                        choices=["all", "kaggle", "hf", "countries", "verify"])
    args = parser.parse_args()

    RAW.mkdir(parents=True, exist_ok=True)

    if args.source in ("hf", "all"):
        download_hf()
    if args.source in ("kaggle", "all"):
        download_kaggle()
    if args.source == "verify":
        pass  # fall through to verify()

    verify()


if __name__ == "__main__":
    main()
