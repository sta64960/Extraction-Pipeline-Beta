-- supabase_setup.sql
-- Initial setup for FFIEC data in Supabase
--
-- IMPORTANT: Since we're uploading ALL FFIEC schedules (not just 3),
-- tables will be created AUTOMATICALLY when data first arrives.
--
-- Supabase auto-creates tables from JSON inserts if they don't exist.
-- You don't need to pre-define every schedule's schema.
--
-- However, you MAY want to run these commands AFTER first upload
-- to add indexes for faster queries:

-- Example: Add indexes on reporting_period for all tables
-- Run this AFTER your first upload to see which tables were created

DO $$
DECLARE
    tbl text;
BEGIN
    FOR tbl IN 
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public' 
        AND tablename LIKE '%ffiec%'
    LOOP
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_period ON %I (reporting_period)', tbl, tbl);
        RAISE NOTICE 'Created index on %', tbl;
    END LOOP;
END $$;

-- Optional: Add created_at timestamp to track when data was uploaded
-- (Run this AFTER first upload if you want upload timestamps)

DO $$
DECLARE
    tbl text;
BEGIN
    FOR tbl IN 
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public' 
        AND tablename LIKE '%ffiec%'
    LOOP
        BEGIN
            EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS _uploaded_at TIMESTAMPTZ DEFAULT NOW()', tbl);
            RAISE NOTICE 'Added _uploaded_at to %', tbl;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Could not add _uploaded_at to %: %', tbl, SQLERRM;
        END;
    END LOOP;
END $$;
