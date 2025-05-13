"""
Data validation utilities for VC scraper
Provides comprehensive validation for scraped portfolio companies and team members
"""
import re
import logging
from typing import Dict, Any, Optional, List, Tuple
from urllib.parse import urlparse
from datetime import datetime
import hashlib
import json

logger = logging.getLogger(__name__)

class ValidationResult:
    """Result of a validation operation"""
    
    def __init__(self, is_valid: bool = True, errors: List[str] = None, warnings: List[str] = None):
        self.is_valid = is_valid
        self.errors = errors or []
        self.warnings = warnings or []
    
    def add_error(self, error: str):
        """Add a validation error"""
        self.errors.append(error)
        self.is_valid = False
    
    def add_warning(self, warning: str):
        """Add a validation warning"""
        self.warnings.append(warning)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'is_valid': self.is_valid,
            'errors': self.errors,
            'warnings': self.warnings
        }

class CompanyValidator:
    """Validator for portfolio company data"""
    
    # Required fields for a company
    REQUIRED_FIELDS = ['name']
    
    # Optional but recommended fields
    RECOMMENDED_FIELDS = ['website', 'industry', 'description']
    
    # URL patterns that might indicate invalid company websites
    INVALID_URL_PATTERNS = [
        r'^https?://(?:www\.)?(?:facebook|twitter|linkedin|instagram|youtube)\.com',
        r'^mailto:',
        r'^tel:',
        r'\.pdf$',
        r'\.zip$',
        r'\.doc$',
        r'\.docx$'
    ]
    
    # Common company name suffixes
    COMPANY_SUFFIXES = [
        'Inc.', 'LLC', 'Corp.', 'Ltd.', 'Limited', 'Corporation',
        'S.A.', 'B.V.', 'GmbH', 'AG', 'Holdings', 'Group',
        'Co.', 'Company', 'Partners', 'Ventures'
    ]
    
    def validate(self, company_data: Dict[str, Any]) -> ValidationResult:
        """
        Validate company data
        
        Args:
            company_data: Dictionary containing company information
            
        Returns:
            ValidationResult object
        """
        result = ValidationResult()
        
        # Check required fields
        for field in self.REQUIRED_FIELDS:
            if not company_data.get(field):
                result.add_error(f"Missing required field: {field}")
        
        # Check recommended fields
        for field in self.RECOMMENDED_FIELDS:
            if not company_data.get(field):
                result.add_warning(f"Missing recommended field: {field}")
        
        # Validate specific fields
        if company_data.get('name'):
            self._validate_company_name(company_data['name'], result)
        
        if company_data.get('website'):
            self._validate_website_url(company_data['website'], result)
        
        if company_data.get('industry'):
            self._validate_industry(company_data['industry'], result)
        
        if company_data.get('funding_amount'):
            self._validate_funding_amount(company_data['funding_amount'], result)
        
        if company_data.get('description'):
            self._validate_description(company_data['description'], result)
        
        # Check for suspicious patterns
        self._check_suspicious_patterns(company_data, result)
        
        return result
    
    def _validate_company_name(self, name: str, result: ValidationResult):
        """Validate company name"""
        if not name or not name.strip():
            result.add_error("Company name is empty")
            return
        
        name = name.strip()
        
        # Check minimum length
        if len(name) < 2:
            result.add_error("Company name is too short")
        
        # Check maximum length
        if len(name) > 100:
            result.add_warning("Company name is very long")
        
        # Check for common invalid patterns
        invalid_patterns = [
            r'^[.\-_\s]+$',  # Only special characters
            r'^\d+$',        # Only numbers
            r'^[A-Z]+$',     # All uppercase (might be acronym only)
        ]
        
        for pattern in invalid_patterns:
            if re.match(pattern, name):
                result.add_warning(f"Company name has suspicious pattern: {name}")
        
        # Check for placeholder text
        placeholder_patterns = [
            r'(?i)placeholder',
            r'(?i)example',
            r'(?i)test\s*company',
            r'(?i)unnamed',
            r'(?i)unknown'
        ]
        
        for pattern in placeholder_patterns:
            if re.search(pattern, name):
                result.add_error(f"Company name appears to be placeholder text: {name}")
    
    def _validate_website_url(self, url: str, result: ValidationResult):
        """Validate website URL"""
        if not url:
            return
        
        # Check if it's a valid URL structure
        try:
            parsed = urlparse(url)
            if not parsed.scheme or not parsed.netloc:
                result.add_error(f"Invalid URL format: {url}")
                return
        except Exception:
            result.add_error(f"Invalid URL: {url}")
            return
        
        # Check for invalid URL patterns
        for pattern in self.INVALID_URL_PATTERNS:
            if re.match(pattern, url, re.IGNORECASE):
                result.add_warning(f"URL appears to be social media or non-company site: {url}")
        
        # Check for common issues
        if url.endswith('/'):
            result.add_warning("URL ends with slash (might be homepage)")
        
        # Check for localhost or IP addresses
        if 'localhost' in url or re.search(r'\d+\.\d+\.\d+\.\d+', url):
            result.add_error(f"URL appears to be localhost or IP address: {url}")
        
        # Check for reasonable TLD
        valid_tlds = ['.com', '.org', '.net', '.io', '.co', '.ai', '.tech', '.ly', '.it']
        if not any(url.lower().endswith(tld) for tld in valid_tlds):
            result.add_warning(f"URL has unusual TLD: {url}")
    
    def _validate_industry(self, industry: str, result: ValidationResult):
        """Validate industry field"""
        if not industry or not industry.strip():
            return
        
        industry = industry.strip()
        
        # Check length
        if len(industry) < 2:
            result.add_warning("Industry name is very short")
        
        if len(industry) > 50:
            result.add_warning("Industry name is very long")
        
        # Check for common invalid patterns
        if re.match(r'^[,/\-\s]+$', industry):
            result.add_error("Industry contains only separators")
        
        # Check for multiple industries (might need splitting)
        separators = [',', '/', '&', ' and ', ' & ']
        for sep in separators:
            if sep in industry:
                result.add_warning(f"Industry contains multiple categories separated by '{sep}'")
                break
    
    def _validate_funding_amount(self, amount: Any, result: ValidationResult):
        """Validate funding amount"""
        if amount is None:
            return
        
        # Convert to number if string
        if isinstance(amount, str):
            # Try to extract numeric value
            match = re.search(r'[\d,]+\.?\d*', amount.replace('$', '').replace('€', ''))
            if not match:
                result.add_error(f"Cannot extract numeric value from funding amount: {amount}")
                return
            
            try:
                amount = float(match.group().replace(',', ''))
            except ValueError:
                result.add_error(f"Invalid funding amount format: {amount}")
                return
        
        # Check reasonable range
        if amount < 0:
            result.add_error("Funding amount cannot be negative")
        
        if amount > 0 and amount < 1000:
            result.add_warning("Funding amount seems very small (less than $1K)")
        
        if amount > 1000000000000:  # $1 trillion
            result.add_warning("Funding amount seems unreasonably large")
    
    def _validate_description(self, description: str, result: ValidationResult):
        """Validate company description"""
        if not description or not description.strip():
            return
        
        description = description.strip()
        
        # Check length
        if len(description) < 10:
            result.add_warning("Description is very short")
        
        if len(description) > 2000:
            result.add_warning("Description is very long")
        
        # Check for placeholder text
        placeholder_patterns = [
            r'(?i)lorem\s+ipsum',
            r'(?i)placeholder',
            r'(?i)description\s+here',
            r'(?i)todo',
            r'(?i)coming\s+soon'
        ]
        
        for pattern in placeholder_patterns:
            if re.search(pattern, description):
                result.add_error(f"Description contains placeholder text: {description[:50]}...")
        
        # Check for minimal content
        word_count = len(description.split())
        if word_count < 3:
            result.add_error("Description has too few words")
    
    def _check_suspicious_patterns(self, company_data: Dict[str, Any], result: ValidationResult):
        """Check for suspicious data patterns"""
        # Check if all fields are empty/None
        non_empty_fields = [v for v in company_data.values() if v and str(v).strip()]
        if len(non_empty_fields) <= 1:
            result.add_error("Too few non-empty fields")
        
        # Check for exact duplicates in critical fields
        name = company_data.get('name', '').lower().strip()
        website = company_data.get('website', '').lower().strip()
        
        if name and website and name in website:
            result.add_warning("Company name appears in website URL")

class TeamMemberValidator:
    """Validator for team member data"""
    
    # Required fields for a team member
    REQUIRED_FIELDS = ['name']
    
    # Optional but recommended fields
    RECOMMENDED_FIELDS = ['title']
    
    def validate(self, member_data: Dict[str, Any]) -> ValidationResult:
        """
        Validate team member data
        
        Args:
            member_data: Dictionary containing team member information
            
        Returns:
            ValidationResult object
        """
        result = ValidationResult()
        
        # Check required fields
        for field in self.REQUIRED_FIELDS:
            if not member_data.get(field):
                result.add_error(f"Missing required field: {field}")
        
        # Check recommended fields
        for field in self.RECOMMENDED_FIELDS:
            if not member_data.get(field):
                result.add_warning(f"Missing recommended field: {field}")
        
        # Validate specific fields
        if member_data.get('name'):
            self._validate_name(member_data['name'], result)
        
        if member_data.get('title'):
            self._validate_title(member_data['title'], result)
        
        if member_data.get('linkedin_url'):
            self._validate_linkedin_url(member_data['linkedin_url'], result)
        
        if member_data.get('email'):
            self._validate_email(member_data['email'], result)
        
        if member_data.get('photo_url'):
            self._validate_photo_url(member_data['photo_url'], result)
        
        # Check for mixed name/title
        self._check_mixed_name_title(member_data, result)
        
        return result
    
    def _validate_name(self, name: str, result: ValidationResult):
        """Validate person name"""
        if not name or not name.strip():
            result.add_error("Name is empty")
            return
        
        name = name.strip()
        
        # Check minimum length
        if len(name) < 2:
            result.add_error("Name is too short")
        
        # Check maximum length
        if len(name) > 100:
            result.add_warning("Name is very long")
        
        # Check for common title patterns mixed with name
        title_patterns = [
            r',\s*(CEO|CTO|CFO|COO|VP|President|Director|Manager)',
            r'-\s*(CEO|CTO|CFO|COO|VP|President|Director|Manager)',
            r'\(\s*(CEO|CTO|CFO|COO|VP|President|Director|Manager)',
        ]
        
        for pattern in title_patterns:
            if re.search(pattern, name, re.IGNORECASE):
                result.add_warning(f"Name appears to contain title: {name}")
        
        # Check for placeholder patterns
        placeholder_patterns = [
            r'(?i)placeholder',
            r'(?i)example',
            r'(?i)test\s*user',
            r'(?i)unnamed',
            r'(?i)unknown',
            r'(?i)no\s*name'
        ]
        
        for pattern in placeholder_patterns:
            if re.search(pattern, name):
                result.add_error(f"Name appears to be placeholder: {name}")
        
        # Check for suspicious patterns
        if re.match(r'^[A-Z\s]+$', name) and len(name.split()) > 2:
            result.add_warning("Name is all uppercase (might be formatted incorrectly)")
        
        if re.match(r'^[a-z\s]+$', name):
            result.add_warning("Name is all lowercase (might be formatted incorrectly)")
    
    def _validate_title(self, title: str, result: ValidationResult):
        """Validate job title"""
        if not title or not title.strip():
            return
        
        title = title.strip()
        
        # Check length
        if len(title) < 2:
            result.add_warning("Title is very short")
        
        if len(title) > 100:
            result.add_warning("Title is very long")
        
        # Check for placeholder patterns
        placeholder_patterns = [
            r'(?i)placeholder',
            r'(?i)position\s*here',
            r'(?i)title\s*here',
            r'(?i)todo',
            r'(?i)tbd'
        ]
        
        for pattern in placeholder_patterns:
            if re.search(pattern, title):
                result.add_error(f"Title appears to be placeholder: {title}")
    
    def _validate_linkedin_url(self, url: str, result: ValidationResult):
        """Validate LinkedIn URL"""
        if not url:
            return
        
        # Check if it's a valid LinkedIn URL
        if not re.match(r'https?://(?:www\.)?linkedin\.com/in/', url, re.IGNORECASE):
            result.add_error(f"Invalid LinkedIn URL format: {url}")
            return
        
        # Check for common issues
        if 'linkedin.com/in/#' in url:
            result.add_warning("LinkedIn URL contains hash character")
        
        # Check for query parameters (might be tracking)
        if '?' in url:
            result.add_warning("LinkedIn URL contains query parameters")
    
    def _validate_email(self, email: str, result: ValidationResult):
        """Validate email address"""
        if not email:
            return
        
        # Basic email pattern
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        if not re.match(email_pattern, email):
            result.add_error(f"Invalid email format: {email}")
            return
        
        # Check for suspicious patterns
        suspicious_domains = [
            'example.com',
            'test.com',
            'placeholder.com',
            'company.com'
        ]
        
        domain = email.split('@')[1].lower()
        if domain in suspicious_domains:
            result.add_warning(f"Email uses placeholder domain: {email}")
    
    def _validate_photo_url(self, url: str, result: ValidationResult):
        """Validate photo URL"""
        if not url:
            return
        
        # Check if it's a valid URL
        try:
            parsed = urlparse(url)
            if not parsed.scheme or not parsed.netloc:
                result.add_error(f"Invalid photo URL format: {url}")
                return
        except Exception:
            result.add_error(f"Invalid photo URL: {url}")
            return
        
        # Check for image file extensions
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg']
        if not any(url.lower().endswith(ext) for ext in image_extensions):
            result.add_warning("Photo URL doesn't end with image extension")
        
        # Check for placeholder images
        placeholder_patterns = [
            r'placeholder',
            r'default\.(?:jpg|png)',
            r'avatar\.(?:jpg|png)',
            r'profile\.(?:jpg|png)',
            r'no-image',
            r'missing'
        ]
        
        for pattern in placeholder_patterns:
            if re.search(pattern, url, re.IGNORECASE):
                result.add_warning(f"Photo URL appears to be placeholder: {url}")
    
    def _check_mixed_name_title(self, member_data: Dict[str, Any], result: ValidationResult):
        """Check if name and title are mixed together"""
        name = member_data.get('name', '')
        title = member_data.get('title', '')
        
        if not name:
            return
        
        # Common patterns where title is mixed with name
        mixed_patterns = [
            (r'(.+?),\s*(.+)', 'comma'),
            (r'(.+?)\s*-\s*(.+)', 'dash'),
            (r'(.+?)\s*\|\s*(.+)', 'pipe'),
            (r'(.+?)\s*\(\s*(.+?)\s*\)', 'parentheses')
        ]
        
        for pattern, separator_type in mixed_patterns:
            match = re.match(pattern, name)
            if match:
                part1, part2 = match.groups()
                
                # Check if second part looks like a title
                title_keywords = [
                    'CEO', 'CTO', 'CFO', 'COO', 'VP', 'President', 'Director',
                    'Manager', 'Lead', 'Head', 'Senior', 'Junior', 'Principal',
                    'Engineer', 'Designer', 'Developer', 'Analyst', 'Specialist'
                ]
                
                if any(keyword.lower() in part2.lower() for keyword in title_keywords):
                    result.add_warning(
                        f"Name appears to contain title separated by {separator_type}: {name}"
                    )

class CompositeValidator:
    """Composite validator that combines all validation logic"""
    
    def __init__(self):
        self.company_validator = CompanyValidator()
        self.member_validator = TeamMemberValidator()
    
    def validate_company(self, company_data: Dict[str, Any]) -> ValidationResult:
        """Validate portfolio company data"""
        return self.company_validator.validate(company_data)
    
    def validate_team_member(self, member_data: Dict[str, Any]) -> ValidationResult:
        """Validate team member data"""
        return self.member_validator.validate(member_data)
    
    def batch_validate_companies(self, companies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate a batch of companies and return summary statistics
        
        Args:
            companies: List of company data dictionaries
            
        Returns:
            Dictionary with validation summary
        """
        results = []
        valid_count = 0
        error_count = 0
        warning_count = 0
        
        for i, company in enumerate(companies):
            result = self.validate_company(company)
            results.append({
                'index': i,
                'company_name': company.get('name', 'Unknown'),
                'validation': result.to_dict()
            })
            
            if result.is_valid:
                valid_count += 1
            error_count += len(result.errors)
            warning_count += len(result.warnings)
        
        return {
            'total_companies': len(companies),
            'valid_companies': valid_count,
            'invalid_companies': len(companies) - valid_count,
            'total_errors': error_count,
            'total_warnings': warning_count,
            'results': results
        }
    
    def batch_validate_team_members(self, members: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate a batch of team members and return summary statistics
        
        Args:
            members: List of member data dictionaries
            
        Returns:
            Dictionary with validation summary
        """
        results = []
        valid_count = 0
        error_count = 0
        warning_count = 0
        
        for i, member in enumerate(members):
            result = self.validate_team_member(member)
            results.append({
                'index': i,
                'member_name': member.get('name', 'Unknown'),
                'validation': result.to_dict()
            })
            
            if result.is_valid:
                valid_count += 1
            error_count += len(result.errors)
            warning_count += len(result.warnings)
        
        return {
            'total_members': len(members),
            'valid_members': valid_count,
            'invalid_members': len(members) - valid_count,
            'total_errors': error_count,
            'total_warnings': warning_count,
            'results': results
        }

# Create a global validator instance
validator = CompositeValidator()

# Convenience functions
def validate_company(company_data: Dict[str, Any]) -> ValidationResult:
    """Validate a single company"""
    return validator.validate_company(company_data)

def validate_team_member(member_data: Dict[str, Any]) -> ValidationResult:
    """Validate a single team member"""
    return validator.validate_team_member(member_data)

def is_valid_company(company_data: Dict[str, Any]) -> bool:
    """Check if a company is valid (convenience function)"""
    return validate_company(company_data).is_valid

def is_valid_team_member(member_data: Dict[str, Any]) -> bool:
    """Check if a team member is valid (convenience function)"""
    return validate_team_member(member_data).is_valid
