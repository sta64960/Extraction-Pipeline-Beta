# pipeline.py - Orchestrates FFIEC → Supabase pipeline
# This is what run.bat / run.sh calls.

import json
import logging
from datetime import datetime
from pathlib import Path

from extractor import download_and_process_new_quarters
from uploader import upload_quarters

STATE_FILE = Path("state.json")
LOG_FILE   = Path("ffiec_pipeline.log")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ── State ─────────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"uploaded_quarters": [], "last_run": None}


def save_state(uploaded_quarters: list[str]):
    with open(STATE_FILE, "w") as f:
        json.dump({
            "uploaded_quarters": sorted(uploaded_quarters),
            "last_run": datetime.now().isoformat(),
        }, f, indent=2)
    log.info(f"State saved. Total quarters in Supabase: {len(uploaded_quarters)}")


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    log.info("=" * 60)
    log.info("FFIEC → Supabase Pipeline Starting")
    log.info("=" * 60)

    # Load what's already been uploaded
    state    = load_state()
    uploaded = set(state.get("uploaded_quarters", []))
    log.info(f"Quarters already in Supabase: {len(uploaded)}")

    # Step 1: Download + convert any new quarters to Parquet
    newly_extracted = download_and_process_new_quarters(already_processed=uploaded)

    if not newly_extracted:
        log.info("Nothing new to upload. Pipeline complete.")
        return

    # Step 2: Upload new Parquet files to Supabase
    newly_uploaded = upload_quarters(newly_extracted)

    # Step 3: Save updated state
    all_uploaded = sorted(uploaded | set(newly_uploaded))
    save_state(all_uploaded)

    log.info("=" * 60)
    log.info(f"Pipeline complete. {len(newly_uploaded)} new quarter(s) in Supabase.")
    log.info("=" * 60)


if __name__ == "__main__":
    run()
