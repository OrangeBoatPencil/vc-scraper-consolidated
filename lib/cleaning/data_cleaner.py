"""
Enhanced data cleaning functions combining features from V1 Scraper
"""
import re
import hashlib
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
from decimal import Decimal, InvalidOperation
import logging

logger = logging.getLogger(__name__)

# Currency mapping for funding amount parsing
CURRENCY_SYMBOLS = {
    '$': 'USD',
    '€': 'EUR',
    '£': 'GBP',
    '¥': 'JPY',
    '₹': 'INR',
    'CHF': 'CHF',
    'AUD': 'AUD',
    'CAD': 'CAD',
}

class DataCleaner:
    """Enhanced data cleaner with features from V1 Scraper"""
    
    def __init__(self, base_url: str = None):
        self.base_url = base_url
        self.company_suffixes = [
            " Inc.", " Inc", " LLC", " Corp.", " Corporation", 
            " Ltd.", " Limited", ", Inc.", ", Inc", ", LLC", 
            ", Corp.", ", Corporation", ", Ltd.", ", Limited",
            " S.A.", " S.L.", " B.V.", " GmbH", " AG"
        ]
        
        # Comprehensive sector mapping from V1
        self.sector_mapping = {
            # AI and Machine Learning
            "ai": "Artificial Intelligence",
            "artificial intelligence": "Artificial Intelligence",
            "machine learning": "Artificial Intelligence",
            "ml": "Artificial Intelligence",
            "deep learning": "Artificial Intelligence",
            "computer vision": "Artificial Intelligence",
            "nlp": "Artificial Intelligence",
            "natural language processing": "Artificial Intelligence",
            
            # Finance
            "fintech": "Financial Technology",
            "financial": "Financial Technology",
            "financial services": "Financial Technology",
            "financial technology": "Financial Technology",
            "banking": "Financial Technology",
            "payments": "Financial Technology",
            "lending": "Financial Technology",
            "wealth management": "Financial Technology",
            "insurance": "Financial Technology",
            "insurtech": "Financial Technology",
            "regtech": "Financial Technology",
            
            # Healthcare
            "healthtech": "Healthcare Technology",
            "health tech": "Healthcare Technology",
            "health": "Healthcare",
            "healthcare": "Healthcare",
            "biotech": "Biotechnology",
            "biotechnology": "Biotechnology",
            "medtech": "Medical Technology",
            "medical technology": "Medical Technology",
            "medical": "Healthcare",
            "pharmaceuticals": "Healthcare",
            "digital health": "Healthcare Technology",
            "telemedicine": "Healthcare Technology",
            
            # Technology
            "saas": "Software as a Service",
            "software": "Software",
            "cloud": "Cloud Computing",
            "cybersecurity": "Cybersecurity",
            "cyber security": "Cybersecurity",
            "security": "Cybersecurity",
            "data analytics": "Data Analytics",
            "big data": "Data Analytics",
            "blockchain": "Blockchain",
            "crypto": "Cryptocurrency",
            "cryptocurrency": "Cryptocurrency",
            "iot": "Internet of Things",
            "internet of things": "Internet of Things",
            
            # E-commerce
            "ecommerce": "E-commerce",
            "e-commerce": "E-commerce",
            "retail": "Retail",
            "marketplace": "E-commerce",
            "direct to consumer": "Direct to Consumer",
            "d2c": "Direct to Consumer",
            
            # Enterprise
            "enterprise": "Enterprise Software",
            "enterprise software": "Enterprise Software",
            "b2b": "B2B",
            "b2b software": "Enterprise Software",
            "crm": "Enterprise Software",
            "erp": "Enterprise Software",
            "productivity": "Enterprise Software",
            
            # Consumer
            "consumer": "Consumer",
            "b2c": "B2C",
            "gaming": "Gaming",
            "entertainment": "Entertainment",
            "media": "Media",
            "social media": "Social Media",
            "marketplace": "Consumer",
            
            # Industry-specific
            "aerospace": "Aerospace",
            "agriculture": "Agriculture",
            "agtech": "Agriculture Technology",
            "automotive": "Automotive",
            "cleantech": "Clean Technology",
            "energy": "Energy",
            "manufacturing": "Manufacturing",
            "real estate": "Real Estate",
            "proptech": "Property Technology",
            "construction": "Construction",
            "transportation": "Transportation",
            "mobility": "Transportation & Mobility",
            "logistics": "Logistics & Supply Chain",
            "supply chain": "Logistics & Supply Chain",
            "education": "Education",
            "edtech": "Education Technology",
            "foodtech": "Food Technology",
            "food and beverage": "Food & Beverage",
            "travel": "Travel",
            "hospitality": "Travel & Hospitality",
        }
        
        # Title standardization mapping
        self.title_mapping = {
            # Partner variations
            "general partner": "General Partner",
            "managing partner": "Managing Partner",
            "founding partner": "Founding Partner",
            "senior partner": "Senior Partner",
            "partner": "Partner",
            
            # Investment roles
            "principal": "Principal",
            "vice president": "Vice President",
            "vp": "Vice President",
            "senior vice president": "Senior Vice President",
            "svp": "Senior Vice President",
            "director": "Director",
            "managing director": "Managing Director",
            "investment director": "Investment Director",
            
            # Analyst roles
            "analyst": "Analyst",
            "senior analyst": "Senior Analyst",
            "junior analyst": "Junior Analyst",
            "investment analyst": "Investment Analyst",
            "research analyst": "Research Analyst",
            
            # Associate roles
            "associate": "Associate",
            "senior associate": "Senior Associate",
            "investment associate": "Investment Associate",
            
            # Executive roles
            "ceo": "Chief Executive Officer",
            "chief executive officer": "Chief Executive Officer",
            "cfo": "Chief Financial Officer",
            "chief financial officer": "Chief Financial Officer",
            "cto": "Chief Technology Officer",
            "chief technology officer": "Chief Technology Officer",
            "cio": "Chief Investment Officer",
            "chief investment officer": "Chief Investment Officer",
            "coo": "Chief Operating Officer",
            "chief operating officer": "Chief Operating Officer",
            
            # Other roles
            "entrepreneur in residence": "Entrepreneur in Residence",
            "eir": "Entrepreneur in Residence",
            "venture partner": "Venture Partner",
            "operating partner": "Operating Partner",
            "advisor": "Advisor",
            "board member": "Board Member",
            "investor": "Investor",
        }
    
    def clean_text(self, text: Optional[str], max_length: int = None) -> Optional[str]:
        """Clean and normalize text data"""
        if not text:
            return None
        
        # Strip whitespace
        text = text.strip()
        
        # Remove extra whitespaces and newlines
        text = re.sub(r'\s+', ' ', text)
        
        # Remove HTML entities
        text = text.replace('&nbsp;', ' ')
        text = text.replace('&amp;', '&')
        text = text.replace('&lt;', '<')
        text = text.replace('&gt;', '>')
        text = text.replace('&quot;', '"')
        text = text.replace('&#x27;', "'")
        
        # Truncate if max_length specified
        if max_length and len(text) > max_length:
            text = text[:max_length].rstrip() + "..."
        
        return text if text else None
    
    def standardize_company_name(self, name: str) -> str:
        """Remove common suffixes and standardize company names"""
        if not name:
            return ""
        
        name = self.clean_text(name)
        if not name:
            return ""
        
        # Remove common suffixes
        for suffix in self.company_suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
        
        # Remove special characters except dots and hyphens
        name = re.sub(r'[^\w\s\.\-]', '', name).strip()
        
        return name
    
    def standardize_sector(self, sector: str) -> str:
        """Map to standard sector categories"""
        if not sector:
            return "Uncategorized"
        
        # Clean and lowercase for comparison
        cleaned = self.clean_text(sector).lower() if sector else ""
        
        # Handle multiple sectors separated by common delimiters
        if '/' in cleaned or ',' in cleaned or '&' in cleaned:
            sectors = re.split(r'[/,&]', cleaned)
            cleaned = sectors[0].strip()
        
        # Check exact match first
        if cleaned in self.sector_mapping:
            return self.sector_mapping[cleaned]
        
        # Check for partial matches
        for key, value in self.sector_mapping.items():
            if key in cleaned or cleaned in key:
                return value
        
        # If no match found, return title-cased original
        words = cleaned.split()
        if words:
            return " ".join(word.capitalize() for word in words)
        
        return "Uncategorized"
    
    def standardize_title(self, title: str) -> str:
        """Standardize job titles"""
        if not title:
            return ""
        
        # Clean whitespace and convert to lowercase for comparison
        title_lower = self.clean_text(title).lower() if title else ""
        
        # Check exact match
        if title_lower in self.title_mapping:
            return self.title_mapping[title_lower]
        
        # Check for partial matches
        for key, value in self.title_mapping.items():
            if key in title_lower or title_lower in key:
                return value
        
        # If no match, return title-cased original
        return self.clean_text(title).title() if title else ""
    
    def parse_name(self, full_name: str) -> Tuple[str, str]:
        """Parse full name into first and last name components"""
        if not full_name:
            return "", ""
        
        # Clean the name
        full_name = self.clean_text(full_name)
        if not full_name:
            return "", ""
        
        # Split name into parts
        parts = full_name.split()
        
        if len(parts) == 1:
            return parts[0], ""
        elif len(parts) == 2:
            return parts[0], parts[1]
        else:
            # For names with more than 2 parts, assume first part is first name
            # and the rest is the last name
            return parts[0], " ".join(parts[1:])
    
    def extract_name_and_title(self, full_name: str) -> Dict[str, Optional[str]]:
        """Separate name and title when mixed together"""
        if not full_name:
            return {"name": None, "title": None}
        
        # Common patterns to separate name and title
        patterns = [
            r'^(.+?),\s*(.+)$',      # Name, Title
            r'^(.+?)\s*-\s*(.+)$',   # Name - Title
            r'^(.+?)\s+\((.+?)\)$',  # Name (Title)
            r'^(.+?)\s*\|\s*(.+)$',  # Name | Title
        ]
        
        for pattern in patterns:
            match = re.match(pattern, full_name.strip())
            if match:
                name = self.clean_text(match.group(1))
                title = self.clean_text(match.group(2))
                return {"name": name, "title": title}
        
        # If no pattern matches, return the whole string as name
        return {"name": self.clean_text(full_name), "title": None}
    
    def normalize_url(self, url: Optional[str]) -> Optional[str]:
        """Convert relative URLs to absolute URLs and validate"""
        if not url:
            return None
        
        url = url.strip()
        
        # If already absolute URL, validate and return
        if url.startswith(('http://', 'https://')):
            try:
                parsed = urlparse(url)
                # Reconstruct URL to normalize it
                return parsed.geturl()
            except Exception:
                return None
        
        # Convert relative to absolute if base_url is provided
        if self.base_url:
            try:
                return urljoin(self.base_url, url)
            except Exception:
                return None
        
        # If no base URL, assume https
        if not url.startswith(('http://', 'https://')):
            try:
                return urljoin('https://', url)
            except Exception:
                return None
        
        return None
    
    def validate_linkedin_url(self, url: str) -> Optional[str]:
        """Validate and normalize LinkedIn URLs"""
        if not url:
            return None
        
        url = url.strip()
        
        # Check if URL contains linkedin.com
        if "linkedin.com" not in url.lower():
            return None
        
        # Ensure URL has proper scheme
        if not url.startswith(('http://', 'https://')):
            url = "https://" + url
        
        # Remove query parameters and fragments
        try:
            parsed = urlparse(url)
            # Remove everything after the path
            url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            
            # Remove trailing slash
            if url.endswith("/"):
                url = url[:-1]
            
            return url
        except Exception:
            return None
    
    def parse_funding_amount(self, amount_str: Optional[str]) -> Tuple[Optional[float], Optional[str]]:
        """Parse funding amounts like '$10M', '€15.5 million', etc."""
        if not amount_str:
            return None, None
        
        amount_str = amount_str.strip()
        
        # Handle cases like "£9 million ($11.6 million)" - take the first part
        if '(' in amount_str:
            amount_str = amount_str.split('(')[0].strip()
        
        # Regex to find currency symbol, amount, and multiplier
        # Handles various formats including numbers with commas
        match = re.match(
            r'([$€£¥₹]|[A-Z]{3})?\s*([\d\.,]+)\s*([mMbBkK]?(?:illion|thousand)?)\s*',
            amount_str, 
            re.IGNORECASE
        )
        
        if not match:
            logger.debug(f"Could not parse funding amount: {amount_str}")
            return None, None
        
        currency_symbol = match.group(1)
        amount_part = match.group(2).replace(',', '')  # Remove commas
        multiplier_part = match.group(3).lower() if match.group(3) else ''
        
        # Determine currency
        currency_code = None
        if currency_symbol:
            if len(currency_symbol) == 3:  # Currency code like CHF, EUR
                currency_code = currency_symbol.upper()
            else:
                currency_code = CURRENCY_SYMBOLS.get(currency_symbol)
        
        # Default to USD if no currency specified
        if not currency_code:
            currency_code = 'USD'
        
        try:
            value = float(amount_part)
            
            # Apply multiplier
            if 'm' in multiplier_part or 'million' in multiplier_part:
                value *= 1_000_000
            elif 'b' in multiplier_part or 'billion' in multiplier_part:
                value *= 1_000_000_000
            elif 'k' in multiplier_part or 'thousand' in multiplier_part:
                value *= 1_000
            
            return value, currency_code
        
        except (ValueError, InvalidOperation) as e:
            logger.warning(f"Could not convert amount part to float: {amount_part} in '{amount_str}': {e}")
            return None, None
    
    def standardize_funding_stage(self, stage: Optional[str]) -> Optional[str]:
        """Standardize funding stage names"""
        if not stage:
            return None
        
        stage_clean = self.clean_text(stage).lower()
        
        # Common stage mappings
        stage_mapping = {
            "pre-seed": "Pre-Seed",
            "pre seed": "Pre-Seed",
            "preseed": "Pre-Seed",
            "seed": "Seed",
            "angel": "Angel",
            "series a": "Series A",
            "series b": "Series B",
            "series c": "Series C",
            "series d": "Series D",
            "series e": "Series E",
            "bridge": "Bridge",
            "growth": "Growth",
            "expansion": "Growth",
            "mezzanine": "Mezzanine",
            "ipo": "IPO",
            "acquisition": "Acquisition",
            "merger": "Merger",
        }
        
        return stage_mapping.get(stage_clean, stage.title())
    
    def extract_location_from_summary(self, summary: str) -> Tuple[Optional[str], str]:
        """Extract location from summary text (from V1 logic)"""
        if not summary:
            return None, summary
        
        # Pattern to find "A City, State-based company" or "City-based company"
        location_pattern = r'^(?:A\s+)?([\w\s.,]+(?:,\s*\w+)?)-based\s+'
        match = re.match(location_pattern, summary, re.IGNORECASE)
        
        if match:
            location = match.group(1).strip().rstrip(',')
            # Remove the location phrase from the summary
            summary_cleaned = re.sub(location_pattern, '', summary, count=1, flags=re.IGNORECASE).strip()
            
            # Capitalize first letter of cleaned summary if needed
            if summary_cleaned:
                summary_cleaned = summary_cleaned[0].upper() + summary_cleaned[1:]
            
            logger.debug(f"Extracted location '{location}' from summary")
            return location, summary_cleaned
        
        return None, summary
    
    def calculate_content_hash(self, data: Dict[str, Any]) -> str:
        """Calculate hash for change detection"""
        # Create a string representation of the data for hashing
        # Sort keys to ensure consistent hashing
        content_str = str(sorted(data.items()))
        return hashlib.md5(content_str.encode()).hexdigest()
    
    def clean_portfolio_company(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean portfolio company data with enhanced logic from V1"""
        cleaned = {}
        
        # Clean name with enhanced standardization
        cleaned['name'] = self.standardize_company_name(raw_data.get('name', ''))
        cleaned['original_name'] = raw_data.get('name', '')
        
        # Standardize sector/industry
        cleaned['sector'] = self.standardize_sector(raw_data.get('sector') or raw_data.get('industry'))
        
        # Handle funding information
        funding_text = raw_data.get('funding') or raw_data.get('funding_description')
        if funding_text:
            amount, currency = self.parse_funding_amount(funding_text)
            cleaned['funding_amount'] = amount
            cleaned['funding_currency'] = currency
            cleaned['funding_description'] = funding_text
        
        # Standardize funding stage
        cleaned['funding_stage'] = self.standardize_funding_stage(raw_data.get('round_type') or raw_data.get('stage'))
        
        # Clean description
        cleaned['description'] = self.clean_text(raw_data.get('description'), max_length=500)
        
        # Normalize URLs
        cleaned['website'] = self.normalize_url(raw_data.get('website') or raw_data.get('url'))
        cleaned['logo_url'] = self.normalize_url(raw_data.get('logo') or raw_data.get('logo_url'))
        
        # Clean location information
        location = raw_data.get('location')
        if not location and cleaned.get('description'):
            # Try to extract location from description
            location, _ = self.extract_location_from_summary(cleaned['description'])
        cleaned['location'] = self.clean_text(location)
        
        # Add metadata
        cleaned['scraped_at'] = datetime.now(timezone.utc).isoformat()
        cleaned['source_url'] = raw_data.get('source_url', self.base_url)
        cleaned['content_hash'] = self.calculate_content_hash(cleaned)
        
        return cleaned
    
    def clean_team_member(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean team member data with enhanced logic from V1"""
        cleaned = {}
        
        # Extract name and title
        full_name = raw_data.get('name', '')
        if full_name:
            name_title = self.extract_name_and_title(full_name)
            cleaned['name'] = name_title['name']
            
            # Use extracted title or provided title
            title = name_title.get('title') or raw_data.get('title', '')
            cleaned['title'] = self.standardize_title(title)
            
            # Parse into first and last names
            if cleaned['name']:
                first_name, last_name = self.parse_name(cleaned['name'])
                cleaned['first_name'] = first_name
                cleaned['last_name'] = last_name
        
        # Clean photo URL
        cleaned['photo_url'] = self.normalize_url(raw_data.get('photo_url') or raw_data.get('image_url'))
        
        # Clean bio/description
        bio = raw_data.get('bio') or raw_data.get('description')
        cleaned['bio'] = self.clean_text(bio, max_length=1000)
        
        # Clean social URLs
        cleaned['linkedin_url'] = self.validate_linkedin_url(raw_data.get('linkedin') or raw_data.get('linkedin_url'))
        cleaned['email'] = self.clean_text(raw_data.get('email'))
        
        # Add metadata
        cleaned['scraped_at'] = datetime.now(timezone.utc).isoformat()
        cleaned['content_hash'] = self.calculate_content_hash(cleaned)
        
        return cleaned
    
    def clean_deal_data(self, raw_data: Dict[str, Any], article_pub_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Clean Fortune deal data with enhanced logic from V1"""
        if not isinstance(raw_data, dict):
            return None
        
        cleaned = {}
        
        # Clean startup name
        startup_name = self.clean_text(raw_data.get('startup_name', ''))
        if not startup_name:
            logger.warning("Deal missing startup name, skipping")
            return None
        cleaned['startup_name'] = startup_name
        
        # Clean website URL
        cleaned['company_website'] = self.normalize_url(raw_data.get('company_website'))
        
        # Handle funding amount
        funding_amount = raw_data.get('funding_amount_description', '')
        if funding_amount:
            amount, currency = self.parse_funding_amount(funding_amount)
            cleaned['funding_amount'] = amount
            cleaned['funding_currency'] = currency
            cleaned['funding_amount_description'] = funding_amount
        
        # Standardize round type
        cleaned['round_type'] = self.standardize_funding_stage(raw_data.get('round_type'))
        
        # Clean investor information
        cleaned['lead_investor'] = self.clean_text(raw_data.get('lead_investor'))
        
        # Clean other investors (filter generic entries)
        other_investors = raw_data.get('other_investors', [])
        if isinstance(other_investors, list):
            generic_filters = {'others', 'angel investors', 'existing investors', 'undisclosed'}
            cleaned['other_investors'] = [
                self.clean_text(inv) for inv in other_investors
                if inv and self.clean_text(inv).lower() not in generic_filters
            ]
        else:
            cleaned['other_investors'] = []
        
        # Handle location and summary
        summary = raw_data.get('summary', '')
        location = raw_data.get('location')
        
        if summary and not location:
            # Try to extract location from summary
            location, summary = self.extract_location_from_summary(summary)
        
        cleaned['location'] = self.clean_text(location)
        cleaned['summary'] = self.clean_text(summary, max_length=250)
        
        # Add source article information
        cleaned['source_article_url'] = raw_data.get('source_article_url')
        cleaned['source_article_title'] = self.clean_text(raw_data.get('source_article_title'))
        
        # Parse article publication date
        if article_pub_date:
            try:
                # Try to parse the date
                if isinstance(article_pub_date, str):
                    # Assume it's already in the correct format or try to parse it
                    cleaned['article_publication_date'] = article_pub_date.strip()
                else:
                    cleaned['article_publication_date'] = str(article_pub_date)
            except Exception as e:
                logger.warning(f"Could not parse article date '{article_pub_date}': {e}")
                cleaned['article_publication_date'] = None
        
        # Add metadata
        cleaned['extracted_at'] = datetime.now(timezone.utc).isoformat()
        cleaned['content_hash'] = self.calculate_content_hash(cleaned)
        
        return cleaned
    
    def validate_company_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate cleaned company data using the advanced validators
        
        Args:
            data: Cleaned company data
            
        Returns:
            True if data is valid, False otherwise
        """
        try:
            from .validators import validate_company
            
            # Use the advanced validator
            result = validate_company(data)
            
            # Log warnings if any
            if result.warnings:
                for warning in result.warnings:
                    logger.warning(f"Company validation warning for '{data.get('name', 'Unknown')}': {warning}")
            
            # Log errors if any
            if result.errors:
                for error in result.errors:
                    logger.error(f"Company validation error for '{data.get('name', 'Unknown')}': {error}")
            
            return result.is_valid
            
        except ImportError:
            # Fallback to basic validation if validator not available
            logger.warning("Advanced validators not available, using basic validation")
            return self._basic_company_validation(data)
    
    def validate_team_member(self, data: Dict[str, Any]) -> bool:
        """
        Validate cleaned team member data using the advanced validators
        
        Args:
            data: Cleaned team member data
            
        Returns:
            True if data is valid, False otherwise
        """
        try:
            from .validators import validate_team_member
            
            # Use the advanced validator
            result = validate_team_member(data)
            
            # Log warnings if any
            if result.warnings:
                for warning in result.warnings:
                    logger.warning(f"Team member validation warning for '{data.get('name', 'Unknown')}': {warning}")
            
            # Log errors if any
            if result.errors:
                for error in result.errors:
                    logger.error(f"Team member validation error for '{data.get('name', 'Unknown')}': {error}")
            
            return result.is_valid
            
        except ImportError:
            # Fallback to basic validation if validator not available
            logger.warning("Advanced validators not available, using basic validation")
            return self._basic_member_validation(data)
    
    def validate_deal_data(self, data: Dict[str, Any]) -> bool:
        """Validate cleaned deal data"""
        # Must have startup name
        if not data.get('startup_name'):
            return False
        
        # Must have funding information
        if not data.get('funding_amount_description') and not data.get('funding_amount'):
            return False
        
        # Must have round type
        if not data.get('round_type'):
            return False
        
        return True
    
    def _basic_company_validation(self, data: Dict[str, Any]) -> bool:
        """Basic company validation (fallback method)"""
        # Must have name
        if not data.get('name'):
            return False
        
        # Name should not be too short
        if len(data.get('name', '')) < 2:
            return False
        
        # Should have at least some additional information
        has_info = any([
            data.get('website'),
            data.get('description'),
            data.get('sector'),
            data.get('funding_amount'),
        ])
        
        return has_info
    
    def _basic_member_validation(self, data: Dict[str, Any]) -> bool:
        """Basic team member validation (fallback method)"""
        # Must have name
        if not data.get('name'):
            return False
        
        # Name should not be too short
        if len(data.get('name', '')) < 2:
            return False
        
        return True
