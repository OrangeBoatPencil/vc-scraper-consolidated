-- Initial schema for VC Scraper
-- Run this in your Supabase SQL Editor

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Sites table for VC firms and companies
CREATE TABLE IF NOT EXISTS sites (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT NOT NULL UNIQUE,
    domain TEXT GENERATED ALWAYS AS (
        CASE 
            WHEN url ~ '^https?://' THEN 
                regexp_replace(url, '^https?://([^/]+).*', '\1')
            ELSE url
        END
    ) STORED,
    description TEXT,
    logo_url TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    last_scraped_at TIMESTAMPTZ,
    active BOOLEAN DEFAULT TRUE,
    scrape_config JSONB DEFAULT '{}'::jsonb
);

-- Portfolio companies table
CREATE TABLE IF NOT EXISTS portfolio_companies (
    id SERIAL PRIMARY KEY,
    site_id INTEGER REFERENCES sites(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    original_name TEXT,
    website TEXT,
    domain TEXT GENERATED ALWAYS AS (
        CASE 
            WHEN website ~ '^https?://' THEN 
                regexp_replace(website, '^https?://([^/]+).*', '\1')
            ELSE website
        END
    ) STORED,
    description TEXT,
    sector TEXT,
    funding_stage TEXT,
    funding_amount NUMERIC,
    funding_currency TEXT DEFAULT 'USD',
    funding_description TEXT,
    location TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    logo_url TEXT,
    scraped_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    first_seen_at TIMESTAMPTZ DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ DEFAULT NOW(),
    source_url TEXT,
    content_hash TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    UNIQUE(site_id, name)
);

-- Team members table
CREATE TABLE IF NOT EXISTS team_members (
    id SERIAL PRIMARY KEY,
    site_id INTEGER REFERENCES sites(id) ON DELETE CASCADE,
    company_id INTEGER REFERENCES portfolio_companies(id) ON DELETE SET NULL,
    name TEXT NOT NULL,
    first_name TEXT,
    last_name TEXT,
    title TEXT,
    standardized_title TEXT,
    bio TEXT,
    photo_url TEXT,
    linkedin_url TEXT,
    twitter_url TEXT,
    email TEXT,
    scraped_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    first_seen_at TIMESTAMPTZ DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ DEFAULT NOW(),
    content_hash TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    UNIQUE(site_id, name)
);

-- Fortune deals table
CREATE TABLE IF NOT EXISTS fortune_deals (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    startup_name TEXT NOT NULL,
    company_website TEXT,
    location TEXT,
    funding_amount_description TEXT,
    funding_amount NUMERIC,
    funding_currency TEXT DEFAULT 'USD',
    round_type TEXT,
    lead_investor TEXT,
    other_investors TEXT[],
    summary TEXT,
    article_publication_date DATE,
    source_article_url TEXT NOT NULL,
    source_article_title TEXT,
    extracted_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    content_hash TEXT,
    UNIQUE(source_article_url, startup_name)
);

-- Scraped pages table (for change tracking)
CREATE TABLE IF NOT EXISTS pages (
    id SERIAL PRIMARY KEY,
    site_id INTEGER REFERENCES sites(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    title TEXT,
    content TEXT,
    metadata JSONB,
    scraped_at TIMESTAMPTZ DEFAULT NOW(),
    hash TEXT,
    page_type TEXT, -- 'portfolio', 'team', 'about', etc.
    UNIQUE(site_id, url)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_portfolio_companies_site_id ON portfolio_companies(site_id);
CREATE INDEX IF NOT EXISTS idx_portfolio_companies_name ON portfolio_companies(name);
CREATE INDEX IF NOT EXISTS idx_portfolio_companies_sector ON portfolio_companies(sector);
CREATE INDEX IF NOT EXISTS idx_portfolio_companies_funding_stage ON portfolio_companies(funding_stage);
CREATE INDEX IF NOT EXISTS idx_portfolio_companies_domain ON portfolio_companies(domain);
CREATE INDEX IF NOT EXISTS idx_portfolio_companies_last_seen ON portfolio_companies(last_seen_at);

CREATE INDEX IF NOT EXISTS idx_team_members_site_id ON team_members(site_id);
CREATE INDEX IF NOT EXISTS idx_team_members_company_id ON team_members(company_id);
CREATE INDEX IF NOT EXISTS idx_team_members_name ON team_members(name);
CREATE INDEX IF NOT EXISTS idx_team_members_title ON team_members(title);
CREATE INDEX IF NOT EXISTS idx_team_members_last_seen ON team_members(last_seen_at);

CREATE INDEX IF NOT EXISTS idx_fortune_deals_startup_name ON fortune_deals(startup_name);
CREATE INDEX IF NOT EXISTS idx_fortune_deals_publication_date ON fortune_deals(article_publication_date);
CREATE INDEX IF NOT EXISTS idx_fortune_deals_round_type ON fortune_deals(round_type);
CREATE INDEX IF NOT EXISTS idx_fortune_deals_funding_amount ON fortune_deals(funding_amount);

CREATE INDEX IF NOT EXISTS idx_pages_site_id ON pages(site_id);
CREATE INDEX IF NOT EXISTS idx_pages_url ON pages(url);
CREATE INDEX IF NOT EXISTS idx_pages_type ON pages(page_type);
CREATE INDEX IF NOT EXISTS idx_pages_scraped_at ON pages(scraped_at);

CREATE INDEX IF NOT EXISTS idx_sites_domain ON sites(domain);
CREATE INDEX IF NOT EXISTS idx_sites_last_scraped ON sites(last_scraped_at);

-- Add full-text search indexes
CREATE INDEX IF NOT EXISTS idx_portfolio_companies_search 
    ON portfolio_companies USING gin(to_tsvector('english', name || ' ' || COALESCE(description, '')));

CREATE INDEX IF NOT EXISTS idx_team_members_search 
    ON team_members USING gin(to_tsvector('english', name || ' ' || COALESCE(title, '') || ' ' || COALESCE(bio, '')));

-- Enable Row Level Security
ALTER TABLE sites ENABLE ROW LEVEL SECURITY;
ALTER TABLE portfolio_companies ENABLE ROW LEVEL SECURITY;
ALTER TABLE team_members ENABLE ROW LEVEL SECURITY;
ALTER TABLE fortune_deals ENABLE ROW LEVEL SECURITY;
ALTER TABLE pages ENABLE ROW LEVEL SECURITY;

-- Create policies for authenticated access
CREATE POLICY "Allow all access for authenticated users" ON sites
    FOR ALL USING (auth.role() = 'authenticated');

CREATE POLICY "Allow all access for authenticated users" ON portfolio_companies
    FOR ALL USING (auth.role() = 'authenticated');

CREATE POLICY "Allow all access for authenticated users" ON team_members
    FOR ALL USING (auth.role() = 'authenticated');

CREATE POLICY "Allow all access for authenticated users" ON fortune_deals
    FOR ALL USING (auth.role() = 'authenticated');

CREATE POLICY "Allow all access for authenticated users" ON pages
    FOR ALL USING (auth.role() = 'authenticated');

-- Grant access to service role
GRANT ALL ON ALL TABLES IN SCHEMA public TO service_role;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO service_role;
