# extractor.py - FFIEC Bulk Extractor
# Downloads ALL schedules from FFIEC, converts to Parquet with ZERO transformation.
# No cleaning, no filtering, no data modification — pure passthrough.

import shutil
import time
import zipfile
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

BULK_URL = "https://cdr.ffiec.gov/public/pws/downloadbulkdata.aspx"

BASE_ZIP_DIR     = Path("data/raw/ffiec_zips")
BASE_EXTRACT_DIR = Path("data/raw/ffiec_extracted")
BASE_PARQUET_DIR = Path("data/raw/ffiec_parquet")


# ── Session ───────────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })
    return s


# ── ASP.NET form helpers ──────────────────────────────────────────────────────

def _hidden_inputs(soup):
    data = {}
    for name in ["__VIEWSTATE","__VIEWSTATEGENERATOR","__EVENTVALIDATION",
                 "__EVENTTARGET","__EVENTARGUMENT","__LASTFOCUS"]:
        el = soup.find("input", {"name": name})
        if el and el.get("value") is not None:
            data[name] = el["value"]
    data.setdefault("__EVENTTARGET", "")
    data.setdefault("__EVENTARGUMENT", "")
    data.setdefault("__LASTFOCUS", "")
    return data

def _find_select_by_option_contains(soup, contains_text):
    for sel in soup.find_all("select"):
        if not sel.get("name"): continue
        for opt in sel.find_all("option"):
            if contains_text in opt.get_text(" ", strip=True):
                return sel["name"]
    raise RuntimeError(f"Could not find a <select> containing: {contains_text}")

def _option_value_by_visible_text(soup, select_name, visible_text):
    sel = soup.find("select", {"name": select_name})
    if not sel: raise RuntimeError(f"Select not found: {select_name}")
    for opt in sel.find_all("option"):
        if opt.get_text(" ", strip=True) == visible_text:
            return opt.get("value")
    sample = [o.get_text(" ", strip=True) for o in sel.find_all("option")[:20]]
    raise RuntimeError(f"'{visible_text}' not found in '{select_name}'. Sample: {sample}")

def _option_value_match_period(soup, select_name, period_text):
    sel = soup.find("select", {"name": select_name})
    if not sel: raise RuntimeError(f"Select not found: {select_name}")
    def norm(s):
        parts = s.strip().split("/")
        if len(parts) == 3:
            m, d, y = parts
            try: return f"{int(m)}/{int(d)}/{y}"
            except: return s.strip()
        return s.strip()
    target_norm = norm(period_text.strip())
    for opt in sel.find_all("option"):
        txt = opt.get_text(" ", strip=True)
        if txt == period_text.strip() or norm(txt) == target_norm:
            return txt, opt.get("value")
    sample = [o.get_text(" ", strip=True) for o in sel.find_all("option")[:30]]
    raise RuntimeError(f"Could not match '{period_text}'. Options: {sample}")

def _find_tab_delimited_radio(soup):
    for inp in soup.find_all("input", {"type": "radio"}):
        name, value = inp.get("name"), inp.get("value")
        if not name or value is None: continue
        if "Tab Delimited" in inp.parent.get_text(" ", strip=True):
            return name, value
    for inp in soup.find_all("input", {"type": "radio"}):
        rid, name, value = inp.get("id"), inp.get("name"), inp.get("value")
        if not rid or not name or value is None: continue
        lab = soup.find("label", {"for": rid})
        if lab and "Tab Delimited" in lab.get_text(" ", strip=True):
            return name, value
    raise RuntimeError("Could not find 'Tab Delimited' radio button")

def _find_download_submit(soup):
    for btn in soup.find_all("input", {"type": "submit"}):
        val = (btn.get("value") or "").strip().lower()
        if val == "download" and btn.get("name"):
            return btn["name"], btn.get("value", "Download")
    btn = soup.find("input", {"type": "submit"})
    if btn and btn.get("name"):
        return btn["name"], btn.get("value", "Download")
    raise RuntimeError("Could not find Download button")

def _postback_select_product(session, soup, product_select_name, product_val):
    hidden = _hidden_inputs(soup)
    payload = dict(hidden)
    payload[product_select_name] = product_val
    payload["__EVENTTARGET"] = product_select_name
    payload["__EVENTARGUMENT"] = ""
    headers = {"Referer": BULK_URL, "Origin": "https://cdr.ffiec.gov",
               "Content-Type": "application/x-www-form-urlencoded"}
    time.sleep(0.8)
    r = session.post(BULK_URL, data=payload, headers=headers, timeout=60, allow_redirects=True)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


# ── Dynamic quarter detection ─────────────────────────────────────────────────

def get_available_quarters(max_quarters: int = 12) -> list[str]:
    """
    Scrapes FFIEC and returns available quarters, limited to most recent.
    
    Args:
        max_quarters: Maximum number of quarters to return (default 12 = 3 years)
    """
    print("  Checking FFIEC for available quarters...")
    with _make_session() as s:
        r = s.get(BULK_URL, timeout=60, allow_redirects=True)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        product_select_name = _find_select_by_option_contains(soup, "Call Reports -- Single Period")
        product_val = _option_value_by_visible_text(soup, product_select_name, "Call Reports -- Single Period")
        soup2 = _postback_select_product(s, soup, product_select_name, product_val)

        for sel in soup2.find_all("select"):
            if not sel.get("name"): continue
            opts = [o.get_text(" ", strip=True) for o in sel.find_all("option")]
            date_opts = [o for o in opts if o.count("/") == 2 and o.strip()]
            if date_opts:
                # FFIEC returns newest first, so [:max_quarters] gets most recent
                limited = date_opts[:max_quarters]
                print(f"  ✓ Found {len(date_opts)} total quarters on FFIEC")
                print(f"  ✓ Limiting to most recent {len(limited)} quarters ({limited[-1]} to {limited[0]})")
                return limited
    raise RuntimeError("Could not find any quarter options on FFIEC website")


# ── Download / extract ────────────────────────────────────────────────────────

def download_bulk_call_single_period(period_mmddyyyy, out_zip):
    out_zip.parent.mkdir(parents=True, exist_ok=True)
    with _make_session() as s:
        r = s.get(BULK_URL, timeout=60, allow_redirects=True)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        product_select_name = _find_select_by_option_contains(soup, "Call Reports -- Single Period")
        product_val = _option_value_by_visible_text(soup, product_select_name, "Call Reports -- Single Period")
        soup2 = _postback_select_product(s, soup, product_select_name, product_val)
        period_select_name = None
        for sel in soup2.find_all("select"):
            if not sel.get("name"): continue
            opts = [o.get_text(" ", strip=True) for o in sel.find_all("option")[:40]]
            if any("/" in t for t in opts):
                period_select_name = sel["name"]
                break
        if not period_select_name:
            raise RuntimeError("Could not locate period dropdown after product postback")
        chosen_text, period_val = _option_value_match_period(soup2, period_select_name, period_mmddyyyy)
        format_name, format_val = _find_tab_delimited_radio(soup2)
        download_name, download_val = _find_download_submit(soup2)
        hidden2 = _hidden_inputs(soup2)
        payload = dict(hidden2)
        payload[product_select_name] = product_val
        payload[period_select_name] = period_val
        payload[format_name] = format_val
        payload[download_name] = download_val
        headers = {"Referer": BULK_URL, "Origin": "https://cdr.ffiec.gov",
                   "Content-Type": "application/x-www-form-urlencoded"}
        time.sleep(1.0)
        print(f"  Downloading {chosen_text}...")
        dl = s.post(BULK_URL, data=payload, headers=headers, timeout=300, stream=True, allow_redirects=True)
        dl.raise_for_status()
        with open(out_zip, "wb") as f:
            for chunk in dl.iter_content(chunk_size=1024 * 1024):
                if chunk: f.write(chunk)
    print(f"  ✓ Downloaded: {out_zip}")
    return out_zip

def extract_zip(zip_path, out_dir):
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(out_dir)
    print(f"  ✓ Extracted: {out_dir}")
    return out_dir


# ── Process ALL TSV files → Parquet (NO FILTERING, NO CLEANING) ──────────────

def process_all_schedules(extract_dir: Path, quarter: str, out_dir: Path) -> list[Path]:
    """
    Converts EVERY .txt file in the extracted folder to Parquet.
    ZERO transformation — pure passthrough of raw FFIEC data.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    txt_files = list(extract_dir.glob("*.txt"))
    
    if not txt_files:
        print(f"  ⚠ No .txt files in {extract_dir}")
        return []

    print(f"  Found {len(txt_files)} schedule files to process")
    written = []

    for txt_file in txt_files:
        # Use original filename as table identifier (strip .txt extension)
        schedule_name = txt_file.stem  # e.g., "FFIEC CDR Call Schedule RC" → same name
        
        try:
            # Read TSV exactly as-is, no transformations
            df = pd.read_csv(txt_file, sep="\t", low_memory=False, encoding="latin-1")
            
            # Only add reporting_period column — everything else untouched
            df["reporting_period"] = quarter
            
            # Sanitize filename for filesystem (replace spaces/special chars with underscores)
            safe_name = "".join(c if c.isalnum() else "_" for c in schedule_name)
            out_path = out_dir / f"{safe_name}.parquet"
            
            # Write raw data to Parquet
            df.to_parquet(out_path, index=False)
            written.append(out_path)
            
            print(f"  ✓ {txt_file.name} → {out_path.name} ({len(df):,} rows)")
            del df
            
        except Exception as e:
            print(f"  ✗ Error processing {txt_file.name}: {e}")
            # Continue processing other files even if one fails

    return written


# ── Cleanup ───────────────────────────────────────────────────────────────────

def cleanup_quarter_staging(quarter_slug: str):
    """Deletes ZIP + extracted folder after successful upload."""
    zip_path    = BASE_ZIP_DIR / f"call_{quarter_slug}.zip"
    extract_dir = BASE_EXTRACT_DIR / quarter_slug
    if zip_path.exists():
        zip_path.unlink()
        print(f"  🗑  Deleted ZIP: {zip_path}")
    if extract_dir.exists():
        shutil.rmtree(extract_dir)
        print(f"  🗑  Deleted extracted folder: {extract_dir}")


# ── Main entry point ──────────────────────────────────────────────────────────

def download_and_process_new_quarters(already_processed: set) -> list[str]:
    """
    Downloads and converts ALL schedules from new quarters.
    Returns list of newly processed quarter slugs.
    """
    print("=" * 70)
    print("STEP 1 — FFIEC Extractor: Checking for new quarters")
    print("=" * 70)

    available    = list(reversed(get_available_quarters()))  # oldest → newest
    new_quarters = [q for q in available if q.replace("/", "-") not in already_processed]

    if not new_quarters:
        print("\n✓ All quarters already downloaded.\n")
        return []

    print(f"\nNew quarters to process: {len(new_quarters)}")
    for q in new_quarters:
        print(f"  • {q}")
    print()

    newly_done = []

    for idx, quarter in enumerate(new_quarters, 1):
        print(f"\n[{idx}/{len(new_quarters)}] {quarter}")
        q_slug      = quarter.replace("/", "-")
        zip_path    = BASE_ZIP_DIR / f"call_{q_slug}.zip"
        extract_dir = BASE_EXTRACT_DIR / q_slug
        parquet_dir = BASE_PARQUET_DIR / q_slug

        if not zip_path.exists():
            try:
                download_bulk_call_single_period(quarter, zip_path)
            except Exception as e:
                print(f"  ✗ Download failed: {e}")
                continue
        else:
            print("  ✓ ZIP already exists")

        if not extract_dir.exists():
            try:
                extract_zip(zip_path, extract_dir)
            except Exception as e:
                print(f"  ✗ Extract failed: {e}")
                continue
        else:
            print("  ✓ Already extracted")

        if not parquet_dir.exists():
            written = process_all_schedules(extract_dir, quarter, parquet_dir)
            if not written:
                print("  ⚠ No files processed — skipping")
                continue
        else:
            print("  ✓ Parquet already exists")

        newly_done.append(q_slug)
        time.sleep(1.5)

    print(f"\n✓ {len(newly_done)} quarter(s) ready for upload.")
    return newly_done


if __name__ == "__main__":
    download_and_process_new_quarters(already_processed=set())
