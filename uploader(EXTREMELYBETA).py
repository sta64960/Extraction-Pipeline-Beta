# uploader.py
# Upload raw parquet files to Supabase Storage
# ZERO transformation — uploads files exactly as extracted.

import os
from pathlib import Path
from supabase import create_client
from dotenv import load_dotenv

# ── Load environment variables ────────────────────────────────────────────────
load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
BUCKET = os.environ.get("SUPABASE_BUCKET", "ffiec-raw")

BASE_PARQUET_DIR = Path("data/raw/ffiec_parquet")


# ── Upload a single quarter ───────────────────────────────────────────────────

def upload_quarter_to_storage(quarter_slug: str) -> int:
    """
    Upload all parquet files for one quarter to Supabase Storage.
    Returns number of files uploaded.
    """
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    qdir = BASE_PARQUET_DIR / quarter_slug
    files = sorted(qdir.glob("*.parquet"))

    if not files:
        print(f"⚠ No parquet files for {quarter_slug}")
        return 0

    uploaded = 0

    for fp in files:
        remote_path = f"call_reports/{quarter_slug}/{fp.name}"

        try:
            with open(fp, "rb") as f:
                sb.storage.from_(BUCKET).upload(
                    path=remote_path,
                    file=f,
                    file_options={
                        "upsert": "true",
                        "content-type": "application/octet-stream",
                    },
                )

            uploaded += 1
            print(f"☁️  {fp.name} → {remote_path}")

        except Exception as e:
            print(f"✗ Failed uploading {fp.name}: {e}")
            raise

    return uploaded


# ── Wrapper used by pipeline.py ───────────────────────────────────────────────

def upload_quarters(quarter_slugs: list[str]) -> list[str]:
    """
    Upload multiple quarters to Supabase Storage.
    Returns list of successfully uploaded quarter slugs.
    """
    if not quarter_slugs:
        print("✓ No new quarters to upload.\n")
        return []

    print("\n" + "=" * 70)
    print("STEP 2 — Supabase Storage Uploader")
    print("=" * 70)
    print(f"Quarters to upload: {quarter_slugs}\n")

    successful = []

    for q in quarter_slugs:
        print(f"\n── {q} ──────────────────────────────────────────────")

        try:
            n = upload_quarter_to_storage(q)

            if n > 0:
                successful.append(q)
                print(f"✓ Quarter {q} uploaded ({n} files)")
            else:
                print(f"⚠ Quarter {q}: nothing uploaded")

        except Exception as e:
            print(f"✗ Quarter {q} failed: {e}")
            break   # stop pipeline on first failure

    print(f"\n✓ Upload complete. {len(successful)} quarter(s) uploaded.")
    return successful


# ── Optional direct run test ──────────────────────────────────────────────────

if __name__ == "__main__":
    # Example manual test:
    # Replace with a real quarter folder name you have
    upload_quarters(["03-31-2024"])
