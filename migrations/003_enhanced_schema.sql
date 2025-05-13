-- Enhanced Fortune deals schema and additional features
-- Run this after 002_add_change_tracking.sql

-- Add additional columns to fortune_deals for better tracking
ALTER TABLE fortune_deals ADD COLUMN IF NOT EXISTS investor_count INTEGER GENERATED ALWAYS AS (
    CASE WHEN other_investors IS NOT NULL THEN array_length(other_investors, 1) ELSE 0 END
) STORED;

ALTER TABLE fortune_deals ADD COLUMN IF NOT EXISTS deal_announcement_date DATE;
ALTER TABLE fortune_deals ADD COLUMN IF NOT EXISTS valuation_amount NUMERIC;
ALTER TABLE fortune_deals ADD COLUMN IF NOT EXISTS valuation_currency TEXT DEFAULT 'USD';
ALTER TABLE fortune_deals ADD COLUMN IF NOT EXISTS sector TEXT;
ALTER TABLE fortune_deals ADD COLUMN IF NOT EXISTS deal_status TEXT DEFAULT 'announced'; -- announced, completed, cancelled
ALTER TABLE fortune_deals ADD COLUMN IF NOT EXISTS notes TEXT;

-- Add more funding round types
ALTER TABLE portfolio_companies ADD COLUMN IF NOT EXISTS sub_round TEXT; -- e.g., 'A-1', 'A-2'
ALTER TABLE portfolio_companies ADD COLUMN IF NOT EXISTS investment_date DATE;
ALTER TABLE portfolio_companies ADD COLUMN IF NOT EXISTS valuation NUMERIC;
ALTER TABLE portfolio_companies ADD COLUMN IF NOT EXISTS employee_count INTEGER;
ALTER TABLE portfolio_companies ADD COLUMN IF NOT EXISTS tags TEXT[];

-- Add investor tracking table
CREATE TABLE IF NOT EXISTS investors (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    type TEXT, -- 'vc', 'angel', 'corporate', 'family_office'
    website TEXT,
    description TEXT,
    logo_url TEXT,
    location TEXT,
    fund_size NUMERIC,
    fund_size_currency TEXT DEFAULT 'USD',
    focus_stages TEXT[], -- e.g., ['seed', 'series_a']
    focus_sectors TEXT[],
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);

-- Investment relationships table
CREATE TABLE IF NOT EXISTS investments (
    id SERIAL PRIMARY KEY,
    investor_id INTEGER REFERENCES investors(id) ON DELETE CASCADE,
    company_id INTEGER REFERENCES portfolio_companies(id) ON DELETE CASCADE,
    fortune_deal_id UUID REFERENCES fortune_deals(id) ON DELETE SET NULL,
    round_type TEXT,
    lead_investor BOOLEAN DEFAULT FALSE,
    amount NUMERIC,
    currency TEXT DEFAULT 'USD',
    investment_date DATE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(investor_id, company_id, round_type)
);

-- Scraping errors table for better error tracking
CREATE TABLE IF NOT EXISTS scraping_errors (
    id SERIAL PRIMARY KEY,
    session_id UUID REFERENCES scraping_sessions(id) ON DELETE CASCADE,
    site_id INTEGER REFERENCES sites(id) ON DELETE CASCADE,
    error_type TEXT NOT NULL, -- 'network', 'parsing', 'validation', 'database'
    error_message TEXT NOT NULL,
    url TEXT,
    stack_trace TEXT,
    occurred_at TIMESTAMPTZ DEFAULT NOW(),
    resolved BOOLEAN DEFAULT FALSE,
    resolution_notes TEXT
);

-- Website patterns table for better scraping configuration
CREATE TABLE IF NOT EXISTS website_patterns (
    id SERIAL PRIMARY KEY,
    domain TEXT NOT NULL,
    page_type TEXT NOT NULL, -- 'portfolio', 'team', 'company_detail'
    selectors JSONB NOT NULL, -- CSS/XPath selectors for different elements
    patterns JSONB, -- Regex patterns for data extraction
    custom_logic TEXT, -- Custom scraping logic if needed
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    created_by TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    UNIQUE(domain, page_type)
);

-- Create indexes for new tables
CREATE INDEX IF NOT EXISTS idx_fortune_deals_sector ON fortune_deals(sector);
CREATE INDEX IF NOT EXISTS idx_fortune_deals_status ON fortune_deals(deal_status);
CREATE INDEX IF NOT EXISTS idx_fortune_deals_valuation ON fortune_deals(valuation_amount);
CREATE INDEX IF NOT EXISTS idx_fortune_deals_announcement_date ON fortune_deals(deal_announcement_date);

CREATE INDEX IF NOT EXISTS idx_investors_name ON investors(name);
CREATE INDEX IF NOT EXISTS idx_investors_type ON investors(type);
CREATE INDEX IF NOT EXISTS idx_investors_stages ON investors USING gin(focus_stages);
CREATE INDEX IF NOT EXISTS idx_investors_sectors ON investors USING gin(focus_sectors);

CREATE INDEX IF NOT EXISTS idx_investments_investor_id ON investments(investor_id);
CREATE INDEX IF NOT EXISTS idx_investments_company_id ON investments(company_id);
CREATE INDEX IF NOT EXISTS idx_investments_round_type ON investments(round_type);
CREATE INDEX IF NOT EXISTS idx_investments_date ON investments(investment_date);

CREATE INDEX IF NOT EXISTS idx_scraping_errors_session_id ON scraping_errors(session_id);
CREATE INDEX IF NOT EXISTS idx_scraping_errors_type ON scraping_errors(error_type);
CREATE INDEX IF NOT EXISTS idx_scraping_errors_occurred_at ON scraping_errors(occurred_at);
CREATE INDEX IF NOT EXISTS idx_scraping_errors_resolved ON scraping_errors(resolved);

CREATE INDEX IF NOT EXISTS idx_website_patterns_domain ON website_patterns(domain);
CREATE INDEX IF NOT EXISTS idx_website_patterns_type ON website_patterns(page_type);

-- Enable RLS for new tables
ALTER TABLE investors ENABLE ROW LEVEL SECURITY;
ALTER TABLE investments ENABLE ROW LEVEL SECURITY;
ALTER TABLE scraping_errors ENABLE ROW LEVEL SECURITY;
ALTER TABLE website_patterns ENABLE ROW LEVEL SECURITY;

-- Create policies for new tables
CREATE POLICY "Allow all access for authenticated users" ON investors
    FOR ALL USING (auth.role() = 'authenticated');

CREATE POLICY "Allow all access for authenticated users" ON investments
    FOR ALL USING (auth.role() = 'authenticated');

CREATE POLICY "Allow all access for authenticated users" ON scraping_errors
    FOR ALL USING (auth.role() = 'authenticated');

CREATE POLICY "Allow all access for authenticated users" ON website_patterns
    FOR ALL USING (auth.role() = 'authenticated');

-- Grant access to service role
GRANT ALL ON investors TO service_role;
GRANT ALL ON investments TO service_role;
GRANT ALL ON scraping_errors TO service_role;
GRANT ALL ON website_patterns TO service_role;

-- Create views for analytics
CREATE OR REPLACE VIEW investment_summary AS
SELECT 
    pc.name as company_name,
    pc.sector,
    pc.funding_stage,
    COUNT(i.id) as total_investors,
    SUM(CASE WHEN i.lead_investor THEN 1 ELSE 0 END) as lead_investors,
    SUM(i.amount) as total_funding,
    i.currency,
    MAX(i.investment_date) as latest_investment_date
FROM portfolio_companies pc
LEFT JOIN investments i ON pc.id = i.company_id
GROUP BY pc.id, pc.name, pc.sector, pc.funding_stage, i.currency;

CREATE OR REPLACE VIEW investor_portfolio AS
SELECT 
    inv.name as investor_name,
    inv.type as investor_type,
    COUNT(i.id) as total_investments,
    COUNT(CASE WHEN i.lead_investor THEN 1 END) as lead_investments,
    SUM(i.amount) as total_invested,
    STRING_AGG(DISTINCT pc.sector, ', ') as sectors_invested
FROM investors inv
LEFT JOIN investments i ON inv.id = i.investor_id
LEFT JOIN portfolio_companies pc ON i.company_id = pc.id
GROUP BY inv.id, inv.name, inv.type;

-- Create function to match and link Fortune deals with existing companies
CREATE OR REPLACE FUNCTION link_fortune_deals_to_companies()
RETURNS INTEGER AS $$
DECLARE
    linked_count INTEGER := 0;
    deal_record RECORD;
BEGIN
    -- Find unlinked Fortune deals
    FOR deal_record IN 
        SELECT fd.*, pc.id as company_id
        FROM fortune_deals fd
        LEFT JOIN portfolio_companies pc ON (
            LOWER(fd.startup_name) = LOWER(pc.name) OR
            LOWER(fd.startup_name) = LOWER(pc.original_name) OR
            fd.company_website = pc.website
        )
        WHERE fd.id NOT IN (
            SELECT DISTINCT fortune_deal_id 
            FROM investments 
            WHERE fortune_deal_id IS NOT NULL
        )
        AND pc.id IS NOT NULL
    LOOP
        -- Create or update investment record
        INSERT INTO investments (
            company_id,
            fortune_deal_id,
            round_type,
            amount,
            currency,
            investment_date
        )
        VALUES (
            deal_record.company_id,
            deal_record.id,
            deal_record.round_type,
            deal_record.funding_amount,
            deal_record.funding_currency,
            deal_record.article_publication_date
        )
        ON CONFLICT (company_id, fortune_deal_id) DO UPDATE SET
            updated_at = NOW();
        
        linked_count := linked_count + 1;
    END LOOP;
    
    RETURN linked_count;
END;
$$ LANGUAGE plpgsql;

-- Grant execute on function
GRANT EXECUTE ON FUNCTION link_fortune_deals_to_companies TO service_role;

-- Create function to update company metrics
CREATE OR REPLACE FUNCTION update_company_metrics()
RETURNS VOID AS $$
BEGIN
    -- Update funding totals from investments
    UPDATE portfolio_companies pc SET
        funding_amount = subq.total_funding
    FROM (
        SELECT 
            company_id,
            SUM(amount) as total_funding
        FROM investments
        WHERE amount IS NOT NULL
        GROUP BY company_id
    ) subq
    WHERE pc.id = subq.company_id;
    
    -- Update latest funding round info
    UPDATE portfolio_companies pc SET
        funding_stage = subq.latest_round
    FROM (
        SELECT DISTINCT ON (company_id) 
            company_id,
            round_type as latest_round
        FROM investments
        WHERE investment_date IS NOT NULL
        ORDER BY company_id, investment_date DESC
    ) subq
    WHERE pc.id = subq.company_id;
END;
$$ LANGUAGE plpgsql;

-- Grant execute on function
GRANT EXECUTE ON FUNCTION update_company_metrics TO service_role;
