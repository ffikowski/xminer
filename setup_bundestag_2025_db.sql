-- Run this script as a database superuser to set up the bundestag_2025 tables
-- and grant proper permissions to the marges user

-- First, grant schema permissions (if not already granted)
GRANT USAGE ON SCHEMA public TO marges;
GRANT CREATE ON SCHEMA public TO marges;

-- Create the bundestag_2025_kerg table (overall results - absolute values)
CREATE TABLE IF NOT EXISTS public.bundestag_2025_kerg (
    id BIGSERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_kerg_row UNIQUE (data)
);

-- Create indexes for kerg table
CREATE INDEX IF NOT EXISTS idx_bundestag_2025_kerg_data
    ON public.bundestag_2025_kerg USING GIN (data);

CREATE INDEX IF NOT EXISTS idx_bundestag_2025_kerg_retrieved
    ON public.bundestag_2025_kerg (retrieved_at);

-- Create the bundestag_2025_kerg2 table (flat form - absolute + relative values)
CREATE TABLE IF NOT EXISTS public.bundestag_2025_kerg2 (
    id BIGSERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_kerg2_row UNIQUE (data)
);

-- Create indexes for kerg2 table
CREATE INDEX IF NOT EXISTS idx_bundestag_2025_kerg2_data
    ON public.bundestag_2025_kerg2 USING GIN (data);

CREATE INDEX IF NOT EXISTS idx_bundestag_2025_kerg2_retrieved
    ON public.bundestag_2025_kerg2 (retrieved_at);

-- Grant all permissions on the tables to marges
GRANT SELECT, INSERT, UPDATE, DELETE ON public.bundestag_2025_kerg TO marges;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.bundestag_2025_kerg2 TO marges;

-- Grant usage on the sequences
GRANT USAGE, SELECT ON SEQUENCE public.bundestag_2025_kerg_id_seq TO marges;
GRANT USAGE, SELECT ON SEQUENCE public.bundestag_2025_kerg2_id_seq TO marges;

-- Add comments
COMMENT ON TABLE public.bundestag_2025_kerg IS 'Bundestag 2025 election results - overall results from all areas (absolute values only)';
COMMENT ON TABLE public.bundestag_2025_kerg2 IS 'Bundestag 2025 election results - flat form with absolute and relative values plus differences to previous period';

COMMENT ON COLUMN public.bundestag_2025_kerg.data IS 'Election result data stored as JSONB for flexible schema';
COMMENT ON COLUMN public.bundestag_2025_kerg.retrieved_at IS 'Timestamp when the data was fetched from bundeswahlleiterin.de';

COMMENT ON COLUMN public.bundestag_2025_kerg2.data IS 'Election result data stored as JSONB for flexible schema';
COMMENT ON COLUMN public.bundestag_2025_kerg2.retrieved_at IS 'Timestamp when the data was fetched from bundeswahlleiterin.de';

-- Display success message
SELECT 'Tables bundestag_2025_kerg and bundestag_2025_kerg2 created successfully!' as status;
SELECT 'Permissions granted to marges user!' as status;

-- Show table info
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE tablename LIKE 'bundestag_2025_%'
ORDER BY tablename;
