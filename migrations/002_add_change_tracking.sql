-- Add change tracking tables
-- Run this after 001_initial_schema.sql

-- Company changes tracking table
CREATE TABLE IF NOT EXISTS company_changes (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES portfolio_companies(id) ON DELETE CASCADE,
    changes JSONB NOT NULL,
    previous_hash TEXT,
    new_hash TEXT,
    changed_at TIMESTAMPTZ DEFAULT NOW(),
    detected_by TEXT DEFAULT 'scraper'
);

-- Team member changes tracking table
CREATE TABLE IF NOT EXISTS member_changes (
    id SERIAL PRIMARY KEY,
    member_id INTEGER NOT NULL REFERENCES team_members(id) ON DELETE CASCADE,
    changes JSONB NOT NULL,
    previous_hash TEXT,
    new_hash TEXT,
    changed_at TIMESTAMPTZ DEFAULT NOW(),
    detected_by TEXT DEFAULT 'scraper'
);

-- Page changes tracking table
CREATE TABLE IF NOT EXISTS page_changes (
    id SERIAL PRIMARY KEY,
    page_id INTEGER NOT NULL REFERENCES pages(id) ON DELETE CASCADE,
    previous_content TEXT,
    new_content TEXT,
    previous_hash TEXT,
    new_hash TEXT,
    changed_at TIMESTAMPTZ DEFAULT NOW(),
    change_type TEXT, -- 'content', 'structure', 'metadata'
    summary TEXT
);

-- Scraping sessions table for tracking scrape runs
CREATE TABLE IF NOT EXISTS scraping_sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    site_id INTEGER REFERENCES sites(id) ON DELETE CASCADE,
    session_type TEXT NOT NULL, -- 'portfolio', 'team', 'full'
    status TEXT NOT NULL DEFAULT 'running', -- 'running', 'completed', 'failed', 'cancelled'
    started_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    duration_seconds INTEGER,
    pages_scraped INTEGER DEFAULT 0,
    companies_found INTEGER DEFAULT 0,
    team_members_found INTEGER DEFAULT 0,
    changes_detected INTEGER DEFAULT 0,
    errors JSONB DEFAULT '[]'::jsonb,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Create indexes for change tracking tables
CREATE INDEX IF NOT EXISTS idx_company_changes_company_id ON company_changes(company_id);
CREATE INDEX IF NOT EXISTS idx_company_changes_changed_at ON company_changes(changed_at);
CREATE INDEX IF NOT EXISTS idx_company_changes_hash ON company_changes(new_hash);

CREATE INDEX IF NOT EXISTS idx_member_changes_member_id ON member_changes(member_id);
CREATE INDEX IF NOT EXISTS idx_member_changes_changed_at ON member_changes(changed_at);
CREATE INDEX IF NOT EXISTS idx_member_changes_hash ON member_changes(new_hash);

CREATE INDEX IF NOT EXISTS idx_page_changes_page_id ON page_changes(page_id);
CREATE INDEX IF NOT EXISTS idx_page_changes_changed_at ON page_changes(changed_at);
CREATE INDEX IF NOT EXISTS idx_page_changes_type ON page_changes(change_type);

CREATE INDEX IF NOT EXISTS idx_scraping_sessions_site_id ON scraping_sessions(site_id);
CREATE INDEX IF NOT EXISTS idx_scraping_sessions_type ON scraping_sessions(session_type);
CREATE INDEX IF NOT EXISTS idx_scraping_sessions_status ON scraping_sessions(status);
CREATE INDEX IF NOT EXISTS idx_scraping_sessions_started_at ON scraping_sessions(started_at);

-- Enable RLS for change tracking tables
ALTER TABLE company_changes ENABLE ROW LEVEL SECURITY;
ALTER TABLE member_changes ENABLE ROW LEVEL SECURITY;
ALTER TABLE page_changes ENABLE ROW LEVEL SECURITY;
ALTER TABLE scraping_sessions ENABLE ROW LEVEL SECURITY;

-- Create policies for change tracking tables
CREATE POLICY "Allow all access for authenticated users" ON company_changes
    FOR ALL USING (auth.role() = 'authenticated');

CREATE POLICY "Allow all access for authenticated users" ON member_changes
    FOR ALL USING (auth.role() = 'authenticated');

CREATE POLICY "Allow all access for authenticated users" ON page_changes
    FOR ALL USING (auth.role() = 'authenticated');

CREATE POLICY "Allow all access for authenticated users" ON scraping_sessions
    FOR ALL USING (auth.role() = 'authenticated');

-- Grant access to service role
GRANT ALL ON company_changes TO service_role;
GRANT ALL ON member_changes TO service_role;
GRANT ALL ON page_changes TO service_role;
GRANT ALL ON scraping_sessions TO service_role;

-- Create views for easier data analysis
CREATE OR REPLACE VIEW recent_company_changes AS
SELECT 
    cc.*,
    pc.name as company_name,
    s.name as site_name
FROM company_changes cc
JOIN portfolio_companies pc ON cc.company_id = pc.id
JOIN sites s ON pc.site_id = s.id
WHERE cc.changed_at > NOW() - INTERVAL '30 days'
ORDER BY cc.changed_at DESC;

CREATE OR REPLACE VIEW recent_member_changes AS
SELECT 
    mc.*,
    tm.name as member_name,
    tm.title,
    s.name as site_name
FROM member_changes mc
JOIN team_members tm ON mc.member_id = tm.id
JOIN sites s ON tm.site_id = s.id
WHERE mc.changed_at > NOW() - INTERVAL '30 days'
ORDER BY mc.changed_at DESC;

-- Create function to clean up old change records
CREATE OR REPLACE FUNCTION cleanup_old_changes(days_to_keep INTEGER DEFAULT 90)
RETURNS INTEGER AS $$
DECLARE
    rows_deleted INTEGER := 0;
    cutoff_date TIMESTAMPTZ;
BEGIN
    cutoff_date := NOW() - INTERVAL '%d days' % days_to_keep;
    
    -- Clean up company changes
    DELETE FROM company_changes WHERE changed_at < cutoff_date;
    GET DIAGNOSTICS rows_deleted = ROW_COUNT;
    
    -- Clean up member changes
    DELETE FROM member_changes WHERE changed_at < cutoff_date;
    GET DIAGNOSTICS rows_deleted = rows_deleted + ROW_COUNT;
    
    -- Clean up page changes
    DELETE FROM page_changes WHERE changed_at < cutoff_date;
    GET DIAGNOSTICS rows_deleted = rows_deleted + ROW_COUNT;
    
    -- Clean up old scraping sessions
    DELETE FROM scraping_sessions WHERE started_at < cutoff_date;
    GET DIAGNOSTICS rows_deleted = rows_deleted + ROW_COUNT;
    
    RETURN rows_deleted;
END;
$$ LANGUAGE plpgsql;

-- Grant execute on function to service role
GRANT EXECUTE ON FUNCTION cleanup_old_changes TO service_role;
