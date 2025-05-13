"""
Team member scraper with enhanced logic and circuit breaker integration
"""
import re
from typing import Dict, List, Any, Optional
import logging
from bs4 import BeautifulSoup, Tag

from .base_scraper import BaseScraper
from ..utils.config import ScrapingConfig
from ..utils.retry_logic import with_retry, CircuitBreaker

logger = logging.getLogger(__name__)

class TeamScraper(BaseScraper):
    """Scraper for team members with circuit breaker protection"""
    
    def __init__(self, config: ScrapingConfig = None):
        super().__init__(config)
        
        # Common selectors for team members
        self.member_selectors = [
            '.team-member',
            '.team-item',
            '.member',
            '.person',
            '.employee',
            '.staff-member',
            '.team-card',
            '.bio-card',
            '.founder',
            '.partner',
            '.team .person',
            '.team .member',
            '[data-member]',
        ]
        
        # Common selectors for member details
        self.detail_selectors = {
            'name': [
                '.name',
                '.person-name',
                '.member-name',
                '.team-member-name',
                '.bio-name',
                '.full-name',
                'h2',
                'h3',
                'h4',
                '.title',
                '[data-name]',
            ],
            'title': [
                '.title',
                '.position',
                '.role',
                '.job-title',
                '.member-title',
                '.job-role',
                '.designation',
                '.rank',
                '.member-position',
            ],
            'bio': [
                '.bio',
                '.description',
                '.member-bio',
                '.bio-text',
                '.about',
                '.summary',
                '.overview',
                '.member-description',
                'p',
            ],
            'photo': [
                'img',
                '.photo img',
                '.member-photo img',
                '.headshot img',
                '.avatar img',
                '.team-photo img',
            ],
            'linkedin': [
                'a[href*="linkedin"]',
                '.linkedin',
                '.linkedin-link',
                '.social-linkedin',
                '[data-linkedin]',
            ],
            'twitter': [
                'a[href*="twitter"]',
                'a[href*="x.com"]',
                '.twitter',
                '.twitter-link',
                '.social-twitter',
                '[data-twitter]',
            ],
            'email': [
                'a[href^="mailto:"]',
                '.email',
                '.email-link',
                '[data-email]',
            ],
        }
        
        # Initialize circuit breaker for team page fetching
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            timeout=300.0,  # 5 minutes
            expected_exception=Exception
        )
    
    @CircuitBreaker(failure_threshold=3, timeout=60.0)
    async def scrape(self, url: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Main scraping method for team members with circuit breaker protection
        
        Args:
            url: Team page URL
            **kwargs: Additional arguments
            
        Returns:
            List of team member dictionaries
        """
        return await self.scrape_with_fallback(url, **kwargs)
    
    @with_retry(max_attempts=3, exceptions=(Exception,))
    async def scrape_team_page(self, url: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Scrape team members from a team page
        
        Args:
            url: Team page URL
            **kwargs: Additional arguments
            
        Returns:
            List of team member dictionaries
        """
        return await self.scrape(url, **kwargs)
    
    async def _parse_page(self, soup: BeautifulSoup, url: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Parse team page and extract member information
        
        Args:
            soup: BeautifulSoup object
            url: Original URL
            **kwargs: Additional arguments
            
        Returns:
            List of member dictionaries
        """
        members = []
        
        # Try different selectors to find team members
        member_elements = []
        for selector in self.member_selectors:
            elements = soup.select(selector)
            if elements and len(elements) > 1:  # We expect multiple members
                member_elements = elements
                logger.debug(f"Found {len(elements)} members using selector: {selector}")
                break
        
        if not member_elements:
            # Fallback: try to find any container with multiple items that look like people
            logger.warning(f"No members found with standard selectors. Trying fallback approach.")
            member_elements = self._fallback_member_detection(soup)
        
        # Extract information from each member element
        for element in member_elements:
            try:
                member_data = await self._extract_member_info(element, url)
                if member_data and member_data.get('name'):
                    members.append(member_data)
            except Exception as e:
                logger.error(f"Error extracting member info: {e}")
                continue
        
        logger.info(f"Extracted {len(members)} team members from {url}")
        return members
    
    def _fallback_member_detection(self, soup: BeautifulSoup) -> List[Tag]:
        """
        Fallback method to detect team members when standard selectors fail
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            List of potential member elements
        """
        # Look for common team page patterns
        containers = soup.select('.team, .about, .founders, .staff, .leadership')
        
        for container in containers:
            # Look for items with photos and names
            items = container.select('div, article, section')
            member_items = []
            
            for item in items:
                # Check if item has both an image and text that looks like a name
                img = item.select_one('img')
                text = item.get_text().strip()
                
                if img and text:
                    # Check if text contains what looks like a person's name
                    if self._looks_like_person_name(text):
                        member_items.append(item)
            
            if len(member_items) > 1:
                return member_items
        
        # Look for img elements with alt text that might be names
        images = soup.select('img[alt]')
        member_items = []
        
        for img in images:
            alt_text = img.get('alt', '')
            if self._looks_like_person_name(alt_text):
                # Get the parent container
                parent = img.parent
                if parent:
                    member_items.append(parent)
        
        return member_items
    
    def _looks_like_person_name(self, text: str) -> bool:
        """
        Determine if text looks like a person's name
        
        Args:
            text: Text to check
            
        Returns:
            Boolean indicating if text looks like a name
        """
        # Clean the text
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Skip very short or very long text
        if len(text) < 4 or len(text) > 100:
            return False
        
        # Check for common name patterns
        # Two words (first and last name)
        words = text.split()
        if len(words) == 2:
            # Both words should start with uppercase
            if all(word[0].isupper() for word in words):
                # No numbers or special characters
                if all(word.isalpha() for word in words):
                    return True
        
        # Check for title patterns like "John Doe, CEO"
        if ',' in text:
            name_part = text.split(',')[0].strip()
            if len(name_part.split()) >= 2:
                return True
        
        # Check for common titles that might be mixed with names
        title_keywords = ['ceo', 'cto', 'cfo', 'founder', 'president', 'director', 'manager', 'partner']
        text_lower = text.lower()
        if any(keyword in text_lower for keyword in title_keywords):
            # Extract the potential name part
            for keyword in title_keywords:
                if keyword in text_lower:
                    parts = re.split(rf'\b{keyword}\b', text_lower, flags=re.IGNORECASE)
                    if parts[0].strip():
                        name_part = parts[0].strip()
                        if len(name_part.split()) >= 2:
                            return True
        
        return False
    
    @CircuitBreaker(failure_threshold=5, timeout=60.0)
    async def _extract_member_info(self, element: Tag, base_url: str) -> Dict[str, Any]:
        """
        Extract member information from a team member element with circuit breaker protection
        
        Args:
            element: BeautifulSoup element containing member info
            base_url: Base URL for resolving relative links
            
        Returns:
            Dictionary with member information
        """
        member = {}
        
        # Extract name and title - they might be combined
        name_raw = self._extract_field(element, 'name')
        if name_raw:
            # Try to separate name and title if they're combined
            parsed = self._parse_name_and_title(name_raw)
            member['name'] = parsed['name']
            if not parsed['title']:
                # If no title was extracted from name, look for separate title field
                member['title'] = self._extract_field(element, 'title')
            else:
                member['title'] = parsed['title']
        else:
            member['name'] = ""
            member['title'] = self._extract_field(element, 'title')
        
        # Extract bio/description
        member['bio'] = self._extract_field(element, 'bio')
        
        # Extract photo
        photo_url = self._extract_photo(element, base_url)
        member['photo_url'] = photo_url
        
        # Extract social media links
        member['linkedin_url'] = self._extract_social_link(element, 'linkedin')
        member['twitter_url'] = self._extract_social_link(element, 'twitter')
        
        # Extract email
        member['email'] = self._extract_email(element)
        
        # Try to extract additional structured data
        additional_info = await self._extract_additional_member_info(element)
        member.update(additional_info)
        
        # Add metadata
        member['source_url'] = base_url
        member['scraped_at'] = self._get_timestamp()
        
        return member
    
    def _extract_field(self, element: Tag, field: str) -> Optional[str]:
        """
        Extract a specific field from an element using multiple selectors
        
        Args:
            element: BeautifulSoup element
            field: Field name ('name', 'title', 'bio', etc.)
            
        Returns:
            Extracted text or None
        """
        selectors = self.detail_selectors.get(field, [])
        
        for selector in selectors:
            found = element.select_one(selector)
            if found:
                text = self.extract_text(found)
                if text and len(text.strip()) > 0:
                    return text.strip()
        
        # Fallback strategies for specific fields
        if field == 'name':
            # Check img alt text
            img = element.select_one('img')
            if img and img.get('alt'):
                return img.get('alt').strip()
            
            # Check data attributes
            for attr in ['data-name', 'data-person-name', 'title']:
                value = element.get(attr)
                if value:
                    return value.strip()
        
        elif field == 'bio':
            # For bio, get all text if no specific selector works
            text = element.get_text()
            if text and len(text.strip()) > 50:  # Assume bio should be substantial
                return text.strip()
        
        return None
    
    def _parse_name_and_title(self, name_raw: str) -> Dict[str, Optional[str]]:
        """
        Parse combined name and title strings
        
        Args:
            name_raw: Raw name string that might contain title
            
        Returns:
            Dictionary with 'name' and 'title' keys
        """
        # Common patterns for name and title combinations
        patterns = [
            r'^(.+?),\s*(.+)$',      # "John Doe, CEO"
            r'^(.+?)\s*-\s*(.+)$',   # "John Doe - CEO"
            r'^(.+?)\s+\((.+?)\)$',  # "John Doe (CEO)"
            r'^(.+?)\s*\|\s*(.+)$',  # "John Doe | CEO"
            r'^(.+?)\s*—\s*(.+)$',   # "John Doe — CEO"
        ]
        
        name_raw = name_raw.strip()
        
        for pattern in patterns:
            match = re.match(pattern, name_raw)
            if match:
                name_part = match.group(1).strip()
                title_part = match.group(2).strip()
                
                # Validate that we have a reasonable name
                if len(name_part.split()) >= 2 and all(word.isalpha() for word in name_part.split()):
                    return {"name": name_part, "title": title_part}
        
        # If no pattern matches, check if it's just a name or if it has title keywords
        words = name_raw.split()
        title_keywords = ['ceo', 'cto', 'cfo', 'founder', 'president', 'director', 'manager', 'partner', 'vp']
        
        # Find title keywords in the string
        title_words = []
        name_words = []
        
        for word in words:
            if word.lower() in title_keywords:
                title_words.append(word)
            else:
                name_words.append(word)
        
        if title_words and name_words:
            return {
                "name": " ".join(name_words).strip(),
                "title": " ".join(title_words).strip().title()
            }
        
        # Default: assume the entire string is the name
        return {"name": name_raw, "title": None}
    
    def _extract_photo(self, element: Tag, base_url: str) -> Optional[str]:
        """
        Extract member photo URL
        
        Args:
            element: BeautifulSoup element
            base_url: Base URL for resolving relative URLs
            
        Returns:
            Photo URL or None
        """
        for selector in self.detail_selectors['photo']:
            img = element.select_one(selector)
            if img:
                # Try different src attributes (some sites use lazy loading)
                for attr in ['src', 'data-src', 'data-lazy-src', 'data-original']:
                    src = img.get(attr)
                    if src:
                        # Skip placeholder images
                        if any(placeholder in src.lower() for placeholder in ['placeholder', 'blank', 'default']):
                            continue
                        
                        return self.parse_absolute_url(src, base_url)
        
        return None
    
    def _extract_social_link(self, element: Tag, platform: str) -> Optional[str]:
        """
        Extract social media links
        
        Args:
            element: BeautifulSoup element
            platform: Platform name ('linkedin', 'twitter')
            
        Returns:
            Social media URL or None
        """
        selectors = self.detail_selectors.get(platform, [])
        
        for selector in selectors:
            link = element.select_one(selector)
            if link:
                href = link.get('href')
                if href:
                    # Validate that it's actually a link to the expected platform
                    href_lower = href.lower()
                    if platform == 'linkedin' and 'linkedin.com' in href_lower:
                        return self._clean_social_url(href)
                    elif platform == 'twitter' and ('twitter.com' in href_lower or 'x.com' in href_lower):
                        return self._clean_social_url(href)
        
        # Look for social links in any links within the element
        all_links = element.select('a[href]')
        for link in all_links:
            href = link.get('href', '').lower()
            if platform == 'linkedin' and 'linkedin.com' in href:
                return self._clean_social_url(link.get('href'))
            elif platform == 'twitter' and ('twitter.com' in href or 'x.com' in href):
                return self._clean_social_url(link.get('href'))
        
        return None
    
    def _clean_social_url(self, url: str) -> str:
        """
        Clean and normalize social media URLs
        
        Args:
            url: Raw social media URL
            
        Returns:
            Cleaned URL
        """
        if not url:
            return ""
        
        # Remove query parameters and fragments
        if '?' in url:
            url = url.split('?')[0]
        if '#' in url:
            url = url.split('#')[0]
        
        # Remove trailing slash
        url = url.rstrip('/')
        
        # Ensure proper protocol
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        return url
    
    def _extract_email(self, element: Tag) -> Optional[str]:
        """
        Extract email address
        
        Args:
            element: BeautifulSoup element
            
        Returns:
            Email address or None
        """
        # Look for mailto links
        for selector in self.detail_selectors['email']:
            link = element.select_one(selector)
            if link:
                href = link.get('href', '')
                if href.startswith('mailto:'):
                    email = href.replace('mailto:', '').split('?')[0]  # Remove any parameters
                    return email.strip()
        
        # Look for email-like text patterns
        text = element.get_text()
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(email_pattern, text)
        
        if emails:
            return emails[0]
        
        return None
    
    async def _extract_additional_member_info(self, element: Tag) -> Dict[str, Any]:
        """
        Extract additional information about the team member
        
        Args:
            element: BeautifulSoup element
            
        Returns:
            Dictionary with additional info
        """
        additional_info = {}
        
        # Look for structured data
        json_ld = element.select('script[type="application/ld+json"]')
        if json_ld:
            try:
                import json
                data = json.loads(json_ld[0].string)
                if isinstance(data, dict):
                    # Extract Person schema data
                    if data.get('@type') == 'Person':
                        additional_info.update(data)
            except:
                pass
        
        # Look for microdata
        if element.get('itemscope'):
            itemtype = element.get('itemtype', '')
            if 'Person' in itemtype:
                for prop_element in element.select('[itemprop]'):
                    prop = prop_element.get('itemprop')
                    if prop:
                        additional_info[prop] = self.extract_text(prop_element)
        
        # Look for data attributes
        for attr in element.attrs:
            if attr.startswith('data-'):
                key = attr.replace('data-', '').replace('-', '_')
                additional_info[key] = element[attr]
        
        # Try to extract additional details from text patterns
        text = element.get_text()
        
        # Extract years of experience
        experience_match = re.search(r'(\d+)\+?\s*(?:years?)\s*(?:of\s*)?(?:experience)', text, re.IGNORECASE)
        if experience_match:
            additional_info['years_experience'] = int(experience_match.group(1))
        
        # Extract education information
        education_keywords = ['university', 'college', 'mba', 'phd', 'bachelor', 'master', 'degree']
        for keyword in education_keywords:
            if keyword.lower() in text.lower():
                # Try to extract the institution name
                pattern = rf'(\w+(?:\s+\w+)*)\s*{keyword}'
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    institution = match.group(1).strip()
                    if institution:
                        additional_info['education'] = additional_info.get('education', [])
                        additional_info['education'].append(institution)
        
        return additional_info
    
    def _get_timestamp(self) -> str:
        """Get current timestamp in ISO format"""
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()
