-- Run this script as a database superuser to set up the bundestag_votes table
-- and grant proper permissions to the marges user

-- First, grant schema permissions
GRANT USAGE ON SCHEMA public TO marges;
GRANT CREATE ON SCHEMA public TO marges;

-- Create the bundestag_votes table
CREATE TABLE IF NOT EXISTS public.bundestag_votes (
    vote_id BIGSERIAL PRIMARY KEY,
    wahlperiode INTEGER NOT NULL,
    sitzungnr INTEGER NOT NULL,
    abstimmnr INTEGER NOT NULL,
    fraktion_gruppe VARCHAR(50),
    name VARCHAR(100) NOT NULL,
    vorname VARCHAR(100),
    titel VARCHAR(50),
    bezeichnung TEXT,
    ja INTEGER NOT NULL DEFAULT 0,
    nein INTEGER NOT NULL DEFAULT 0,
    enthaltung INTEGER NOT NULL DEFAULT 0,
    ungueltig INTEGER NOT NULL DEFAULT 0,
    nichtabgegeben INTEGER NOT NULL DEFAULT 0,
    bemerkung TEXT,
    vote_title TEXT,
    vote_date DATE,
    vote_source_url TEXT,
    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_vote_member UNIQUE (wahlperiode, sitzungnr, abstimmnr, name, vorname),
    CONSTRAINT check_single_vote CHECK (
        (ja + nein + enthaltung + ungueltig + nichtabgegeben) = 1
    )
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_bundestag_votes_vote_session
    ON public.bundestag_votes (wahlperiode, sitzungnr, abstimmnr);

CREATE INDEX IF NOT EXISTS idx_bundestag_votes_member
    ON public.bundestag_votes (name, vorname);

CREATE INDEX IF NOT EXISTS idx_bundestag_votes_fraktion
    ON public.bundestag_votes (fraktion_gruppe);

CREATE INDEX IF NOT EXISTS idx_bundestag_votes_retrieved
    ON public.bundestag_votes (retrieved_at);

CREATE INDEX IF NOT EXISTS idx_bundestag_votes_title
    ON public.bundestag_votes (vote_title);

CREATE INDEX IF NOT EXISTS idx_bundestag_votes_date
    ON public.bundestag_votes (vote_date);

-- Grant all permissions on the table to marges
GRANT SELECT, INSERT, UPDATE, DELETE ON public.bundestag_votes TO marges;

-- Grant usage on the sequence
GRANT USAGE, SELECT ON SEQUENCE public.bundestag_votes_vote_id_seq TO marges;

-- Add comments
COMMENT ON TABLE public.bundestag_votes IS 'Individual Bundestag member votes from plenary sessions';
COMMENT ON COLUMN public.bundestag_votes.wahlperiode IS 'Electoral period number';
COMMENT ON COLUMN public.bundestag_votes.sitzungnr IS 'Session number within the electoral period';
COMMENT ON COLUMN public.bundestag_votes.abstimmnr IS 'Vote number within the session';
COMMENT ON COLUMN public.bundestag_votes.vote_title IS 'Title/topic of the law or motion being voted on';
COMMENT ON COLUMN public.bundestag_votes.vote_date IS 'Date when the vote took place';
COMMENT ON COLUMN public.bundestag_votes.vote_source_url IS 'URL to the Excel file on bundestag.de';

-- Display success message
SELECT 'Table bundestag_votes created successfully and permissions granted to marges!' as status;
