# FFIEC → Supabase Pipeline

**Zero transformation, pure data passthrough.**  
Downloads ALL FFIEC Call Report schedules and uploads them raw to Supabase.

---

## What This Does

1. **Scrapes FFIEC** for available quarters (auto-detects, no hardcoding)
2. **Downloads new quarters** only (skips what's already in Supabase)
3. **Converts TSV → Parquet** (temporary local staging)
4. **Uploads to Supabase** (every schedule becomes a table)
5. **Cleans up** local files after successful upload
6. **Tracks state** so it never uploads the same quarter twice

**Zero data cleaning or transformation** — FFIEC data goes into Supabase exactly as-is.

---

## First-Time Setup (5 minutes)

### Step 1: Install Python

- **Windows:** Download from [python.org](https://www.python.org/downloads/)
- **Mac:** Already installed, or `brew install python3`
- **Linux:** `sudo apt install python3 python3-pip`

Verify: open terminal/command prompt and run `python --version` (should be 3.11+)

### Step 2: Install Dependencies

Open terminal in this project folder and run:

```bash
pip install -r requirements.txt
```

### Step 3: Set Up Supabase

1. Go to your Supabase project dashboard
2. Click **Settings** → **API**
3. Copy these two values:
   - **Project URL** (e.g., `https://abc123.supabase.co`)
   - **service_role key** (the secret one, NOT the anon key)

4. Open `config.py` and paste your values:
   ```python
   SUPABASE_URL = "https://abc123.supabase.co"
   SUPABASE_KEY = "eyJ...your-service-role-key..."
   ```

5. Save `config.py`

### Step 4: (Optional) Run Setup SQL

The pipeline will auto-create tables as data arrives. However, if you want to add indexes for faster queries, run `supabase_setup.sql` in the Supabase SQL Editor **AFTER** your first upload completes.

---

## Running the Pipeline

### Windows

Double-click `run.bat`

### Mac / Linux

In terminal:
```bash
chmod +x run.sh  # First time only
./run.sh
```

Or just:
```bash
python3 pipeline.py
```

---

## What Happens When You Run It

```
┌─────────────────────────────────────────────┐
│ STEP 1: Extractor                           │
│ • Scrapes FFIEC for available quarters      │
│ • Downloads new quarter ZIPs                │
│ • Extracts ALL .txt files                   │
│ • Converts each to Parquet (no cleaning)    │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│ STEP 2: Uploader                            │
│ • Reads Parquet files                       │
│ • Uploads to Supabase in batches            │
│ • One table per FFIEC schedule              │
│ • Deletes local ZIP + extracted folder      │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│ Updates state.json                          │
│ Logs everything to ffiec_pipeline.log       │
└─────────────────────────────────────────────┘
```

---

## How Often to Run

**4 times per year**, roughly 45 days after each quarter closes:

| Quarter Ends | FFIEC Publishes | Recommended Run Date |
|--------------|-----------------|----------------------|
| Mar 31 (Q1)  | ~May 15         | Mid-May              |
| Jun 30 (Q2)  | ~Aug 15         | Mid-August           |
| Sep 30 (Q3)  | ~Nov 15         | Mid-November         |
| Dec 31 (Q4)  | ~Feb 15         | Mid-February         |

**Safe to run anytime** — it always skips quarters already in Supabase.

---

## Understanding the Files

```
ffiec-pipeline/
├── pipeline.py           ← Main orchestrator (what run.bat calls)
├── extractor.py          ← Downloads from FFIEC, converts to Parquet
├── uploader.py           ← Pushes Parquet → Supabase
├── config.py             ← Your Supabase credentials (NEVER commit!)
├── run.bat               ← Windows: double-click to run
├── run.sh                ← Mac/Linux: ./run.sh to run
├── requirements.txt      ← Python dependencies
├── supabase_setup.sql    ← Optional: run AFTER first upload for indexes
├── README.md             ← This file
├── .gitignore            ← Prevents committing data/credentials
│
├── state.json            ← Auto-generated: tracks uploaded quarters
├── ffiec_pipeline.log    ← Auto-generated: execution history
│
└── data/raw/             ← Auto-generated: temporary staging
    ├── ffiec_zips/       ← Downloaded ZIPs (deleted after upload)
    ├── ffiec_extracted/  ← Extracted TSV files (deleted after upload)
    └── ffiec_parquet/    ← Converted Parquet files (kept as record)
```

---

## Supabase Table Structure

Each FFIEC schedule becomes one table. Example:

- `FFIEC_CDR_Call_Schedule_RC` ← Balance Sheet
- `FFIEC_CDR_Call_Schedule_RI` ← Income Statement
- `FFIEC_CDR_Call_Demographic` ← Bank metadata
- ... and 20+ more schedules depending on quarter

Every table includes a `reporting_period` column (e.g., `"03/31/2024"`).

---

## Troubleshooting

### "No module named 'requests'" or similar

Run: `pip install -r requirements.txt`

### "Could not connect to Supabase"

Check `config.py` — make sure you used the **service_role key**, not anon key.

### "Quarter partially failed"

Check `ffiec_pipeline.log` for details. The quarter will NOT be marked as uploaded, so you can fix the issue and re-run safely.

### "Already uploaded" but data looks wrong

1. Check `state.json` to see which quarters are tracked
2. Manually delete rows from Supabase if needed
3. Remove the quarter slug from `state.json`
4. Re-run the pipeline

### Files taking up too much space

The pipeline auto-deletes ZIPs and extracted folders after upload. Only Parquet files remain as a lightweight record (~10% of original size). If you want to delete those too:

```bash
# Windows
rmdir /s data\raw\ffiec_parquet

# Mac/Linux
rm -rf data/raw/ffiec_parquet
```

Then edit `state.json` to remove those quarters, and they'll re-download next run.

---

## Important Notes

- **No data transformation**: FFIEC data goes into Supabase exactly as published
- **Idempotent**: Safe to run multiple times, never creates duplicates
- **State-tracked**: `state.json` records what's been uploaded
- **Logged**: Check `ffiec_pipeline.log` if anything goes wrong
- **Automatic cleanup**: Local staging files deleted after successful upload

---

## Contact / Questions

[Add your contact info here for the company]

For technical issues, check `ffiec_pipeline.log` first — it contains detailed error messages.
