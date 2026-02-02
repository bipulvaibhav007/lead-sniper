import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
import dns.resolver

# Regex patterns
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
GENERIC_PREFIXES = ['info', 'contact', 'admin', 'support', 'hello', 'office', 'sales', 'enquiries', 'team']
SOCIAL_PATTERNS = {
    "facebook": r'facebook\.com\/[a-zA-Z0-9\.]+',
    "instagram": r'instagram\.com\/[a-zA-Z0-9_\.]+',
    "linkedin": r'linkedin\.com\/(in|company)\/[a-zA-Z0-9_\-]+',
    "twitter": r'(twitter\.com|x\.com)\/[a-zA-Z0-9_]+'
}

def generate_icebreaker(business):
    """
    Creates a personalized opening line for cold emails based on site data.
    """
    name = business.get('name', 'there')
    title = business.get('site_title', '').strip()
    desc = business.get('site_desc', '').strip()
    
    # 1. Clean up the Title (Remove generic text like "Home", "Welcome")
    # We remove text after separators like | or - to get the core brand/service
    clean_title = re.sub(r'(?i)(home|welcome|page|index|\||-).*', '', title).strip()
    
    # Fallback if title is too short
    if len(clean_title) < 3: 
        clean_title = business.get('name')
    
    # 2. Extract a key service from the description
    # We look for the first meaningful phrase (up to the first punctuation)
    focus = "your services"
    if desc:
        # Split by common punctuation to isolate the first thought
        parts = re.split(r'[.,!]', desc)
        if parts:
            # Take the first part, limit to 50 chars so it flows naturally
            raw_focus = parts[0].strip()
            if raw_focus:
                focus = f"your work in {raw_focus.lower()[:60]}..."
            
    return f"Hi {name}, I was checking out {clean_title} and noticed {focus}"

def verify_domain_mx(email):
    """Checks if the email domain actually has mail servers."""
    try:
        domain = email.split('@')[-1]
        records = dns.resolver.resolve(domain, 'MX')
        return True if records else False
    except:
        return False

def clean_phone(phone_str):
    """Removes junk characters to make phone ready for dialers."""
    if not phone_str or phone_str == "N/A": return ""
    # Keep only digits and plus sign
    cleaned = re.sub(r'[^\d+]', '', phone_str)
    return cleaned

def sort_emails(email_list):
    """Puts personal emails (john@) before generic emails (info@)."""
    personal = []
    generic = []
    for email in email_list:
        prefix = email.split('@')[0].lower()
        if any(g in prefix for g in GENERIC_PREFIXES):
            generic.append(email)
        else:
            personal.append(email)
    return personal + generic

def get_soup(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }
    try:
        if not url.startswith('http'): url = 'http://' + url
        response = requests.get(url, headers=headers, timeout=8)
        if response.status_code == 200:
            return BeautifulSoup(response.text, 'html.parser')
    except:
        return None
    return None

def extract_data_from_soup(soup):
    text = soup.get_text(" ")
    emails = set(EMAIL_RE.findall(text))
    
    socials = {k: [] for k in SOCIAL_PATTERNS}
    for a in soup.find_all('a', href=True):
        href = a['href']
        for network, pattern in SOCIAL_PATTERNS.items():
            if re.search(pattern, href):
                socials[network].append(href)
    
    # Clean up socials
    for k in socials: socials[k] = list(set(socials[k]))
    return emails, socials

def enrich_business_data(business):
    url = business.get('website')
    
    # Initialize fields including new 'icebreaker'
    business.update({
        'emails': "", 'best_email': "", 'email_status': "N/A", 
        'site_title': "", 'site_desc': "", 'icebreaker': "",
        'facebook': "", 'instagram': "", 'linkedin': "", 
        'lead_score': 0, 'clean_phone': clean_phone(business.get('phone', ''))
    })
    
    # 1. Base Score from Maps Data
    score = 0
    if business.get('phone') and business.get('phone') != "N/A": score += 20
    if business.get('address') and business.get('address') != "N/A": score += 10
    
    if not url or url.lower() == 'n/a': 
        business['lead_score'] = score
        return business

    soup = get_soup(url)
    if not soup: 
        business['lead_score'] = score
        return business

    # 2. Get Context (Title/Desc)
    try:
        if soup.title: business['site_title'] = soup.title.string.strip()
        meta = soup.find('meta', attrs={'name': 'description'})
        if meta: business['site_desc'] = meta.get('content', '').strip()
    except: pass

    # 3. Extract Emails & Socials
    emails, socials = extract_data_from_soup(soup)

    # Deep Crawl (Contact Page) if needed
    if not emails:
        for link in soup.find_all('a', href=True):
            if 'contact' in link['href'].lower():
                full_link = urljoin(url, link['href'])
                c_soup = get_soup(full_link)
                if c_soup:
                    e, s = extract_data_from_soup(c_soup)
                    emails.update(e)
                    for k, v in s.items(): socials[k].extend(v)
                break

    # 4. Process Emails
    valid_emails = [e for e in emails if e.split('.')[-1] not in ['png','jpg','js','css']]
    sorted_emails = sort_emails(valid_emails)
    
    business['emails'] = ", ".join(sorted_emails)
    if sorted_emails:
        business['best_email'] = sorted_emails[0] # The one they should email first
        score += 40 # Huge points for finding an email
        
        # Domain Check
        if verify_domain_mx(sorted_emails[0]):
            business['email_status'] = "Verified"
            score += 10
        else:
            business['email_status'] = "Unverified"

    # 5. Process Socials
    business['facebook'] = ", ".join(socials['facebook'])
    business['instagram'] = ", ".join(socials['instagram'])
    business['linkedin'] = ", ".join(socials['linkedin'])
    
    if business['facebook']: score += 10
    if business['linkedin']: score += 10
    
    # 6. Generate Icebreaker (New Feature)
    business['icebreaker'] = generate_icebreaker(business)
    
    business['lead_score'] = min(score, 100) # Cap at 100
    
    return business