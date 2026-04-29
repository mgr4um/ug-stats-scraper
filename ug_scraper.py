from curl_cffi import requests
import cloudscraper
from bs4 import BeautifulSoup
import sqlite3
import re
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import time
import json

class UGStatsScraper:
    def __init__(self, db_path: str = "ug_stats.db"):
        """Initialize scraper with database path"""
        self.session = cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'desktop': True
        }
    )
        self.db_path = db_path
        self.base_url = "https://www.ultimate-guitar.com"
        
        # Set headers to mimic a real browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        })
        
        self.init_database()
    
    def init_database(self):
        """Create database tables if they don't exist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Table for tab information
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tabs (
                tab_id INTEGER PRIMARY KEY,
                artist TEXT NOT NULL,
                song_name TEXT NOT NULL,
                tab_type TEXT,
                url TEXT UNIQUE,
                date_submitted TEXT,
                status TEXT
            )
        ''')
        
        # Table for stats snapshots
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stats_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tab_id INTEGER,
                timestamp TEXT NOT NULL,
                views INTEGER,
                rating_stars REAL,
                rating_count INTEGER,
                favorites INTEGER,
                FOREIGN KEY (tab_id) REFERENCES tabs(tab_id)
            )
        ''')
        
        # Index for faster queries
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_stats_timestamp 
            ON stats_history(tab_id, timestamp)
        ''')
        
        conn.commit()
        conn.close()
    
    def login(self, cookies: Dict[str, str]):
        """
        Login using cookies from your browser
        
        Args:
            cookies: Dictionary of cookies from your logged-in browser session
                    Required cookies: bbuserid, bbusername, bbpassword, UGSESSION
        """
        # Ensure the session is using curl_cffi with browser impersonation
        # If you initialize this in __init__, you can skip this line
        self.session = requests.Session(impersonate="chrome120")

        # Update session with provided cookies
        for key, value in cookies.items():
            self.session.cookies.set(key, value, domain='.ultimate-guitar.com')
        
        # Also set for www subdomain
        for key, value in cookies.items():
            self.session.cookies.set(key, value, domain='www.ultimate-guitar.com')
        
        # Set Referer header (curl_cffi will handle the rest of the browser headers)
        self.session.headers.update({
            'Referer': 'https://www.ultimate-guitar.com/'
        })

        # Verify login by checking contribution page
        try:
            # Use user_id and username from cookies
            user_id = cookies.get('bbuserid')
            user_name = cookies.get('bbusername')

            if not user_id or not user_name:
                print("✗ Missing bbuserid or bbusername in cookies")
                return False

            # curl_cffi handles the GET request with browser-like TLS fingerprinting
            response = self.session.get(f"{self.base_url}/contribution/{user_id}-{user_name}/tabs", timeout=10)
            
            if response.status_code == 403:
                print("✗ Login failed - check your cookies")
                print("  HTTP status: 403")
                print("  Response snippet:", response.text[:500])
                print("\n💡 Tips:")
                print("  1. Make sure you're copying ALL cookie values correctly")
                print("  2. Extract fresh cookies while logged in")
                print("  3. Check if you can access the page manually in your browser")
                print("  4. Ultimate Guitar might be blocking automated requests")
                return False
            
            if response.status_code != 200:
                print(f"✗ Login failed - HTTP {response.status_code}")
                return False
                
            if "My contributions" in response.text or "contribution" in response.text.lower():
                print("✓ Login successful")
                return True
            else:
                print("✗ Login failed - couldn't verify login")
                print("  Page content doesn't show contribution section")
                return False
                
        except requests.errors.RequestException as e:
            print(f"✗ Network error during login: {e}")
            return False
    
    def extract_tab_id(self, url: str) -> Optional[int]:
        """
        Extract tab ID from any UG-style URL (handles /tab/5337825, -5337825, etc.)
        """
        if not url:
            return None
        m = re.search(r'(?:-|/)(\d{5,})(?:$|[/?#])', url)
        return int(m.group(1)) if m else None

    def parse_rating(self, rating_td_html: str) -> tuple:
        """
        Parse rating HTML fragment (td.th--rating) and return (stars_or_None, votes)
        Handles '-' (no rating) and both full/half stars.
        """
        soup = BeautifulSoup(rating_td_html or "", 'html.parser')
        text = soup.get_text(strip=True)
        if not text or text in ('-', '—'):
            return (None, 0)

        # Count star elements reliably (handles classes like "fa fa-star")
        full = len(soup.select('span.fa-star'))
        half = len(soup.select('span.fa-star-half-o'))
        stars = full + 0.5 * half

        m = re.search(r'(\d+)', text)
        votes = int(m.group(1)) if m else 0

        return (stars if stars > 0 else None, votes)

    def scrape_page(self, user_id: int, user_name: str, page: int = 1) -> List[Dict]:
        """Scrape a single page of contributions (robust selectors + error handling)"""
        url = f"{self.base_url}/contribution/{user_id}-{user_name}/tabs?page={page}&per-page=50"

        try:
            # Use the curl_cffi session which already has the cookies and browser impersonation
            resp = self.session.get(url, timeout=15)
        except requests.errors.RequestException as e:
            print(f"✗ Network error fetching page {page}: {e}")
            return []

        if resp.status_code != 200:
            print(f"✗ Failed to fetch page {page} (HTTP {resp.status_code})")
            if resp.status_code == 403:
                print("  Hint: Cloudflare might have flagged the session. Try adding a small delay.")
            return []

        # BeautifulSoup works the same, but using resp.content ensures correct encoding
        soup = BeautifulSoup(resp.content, 'html.parser')
        rows = soup.select('tr.b-tab-info')
        
        tabs = []

        for row in rows:
            try:
                main = row.select_one('span.b-tab-info--main')
                if not main:
                    continue

                # Artist: first text node (before <a>) if present
                artist = ''
                if main.contents:
                    first = main.contents[0]
                    artist = first.strip() if isinstance(first, str) else main.get_text(separator='|', strip=True).split('|')[0].strip()
                artist = artist.replace('—', '').strip()

                link = main.find('a')
                song_name = link.get_text(strip=True) if link else ''
                tab_url = link['href'] if link and link.has_attr('href') else None
                full_url = f"{self.base_url}{tab_url}" if tab_url and tab_url.startswith('/') else tab_url

                tab_id = self.extract_tab_id(tab_url or '') or self.extract_tab_id(full_url or '')

                # tab type (second td usually)
                tab_type = (row.select_one('td:nth-of-type(2)') or row.select_one('td')).get_text(strip=True)

                rating_td = row.select_one('td.th--rating')
                stars, votes = self.parse_rating(str(rating_td)) if rating_td else (None, 0)

                date_td = row.select_one('td.th--date')
                date_submitted = date_td.get_text(strip=True) if date_td else None

                # views and favorites from columns (defensive parsing)
                tds = row.find_all('td')
                views = 0
                favorites = 0
                try:
                    if len(tds) > 4:
                        views = int(tds[4].get_text(strip=True).replace(',', ''))
                    if len(tds) > 5:
                        favorites = int(tds[5].get_text(strip=True).replace(',', ''))
                except ValueError:
                    pass

                status = (row.select_one('span.label') and row.select_one('span.label').get_text(strip=True)) or 'Unknown'

                tabs.append({
                    'tab_id': tab_id,
                    'artist': artist,
                    'song_name': song_name,
                    'tab_type': tab_type,
                    'url': full_url,
                    'date_submitted': date_submitted,
                    'status': status,
                    'views': views,
                    'rating_stars': stars,
                    'rating_count': votes,
                    'favorites': favorites
                })
            except Exception as e:
                # If a row fails, print the error and the row snippet to help debug
                print(f"✗ row parse error: {e}")
                try:
                    print(row.prettify()[:1000])
                except Exception:
                    pass
                continue

        return tabs

    
    def scrape_all_pages(self, user_id: int, user_name) -> List[Dict]:
        """Scrape all pages of contributions"""
        all_tabs = []
        page = 1
        
        print("Scraping contributions...")
        while True:
            print(f"  Page {page}...", end=' ')
            tabs = self.scrape_page(user_id, user_name, page)
            
            if not tabs:
                print("(no more data)")
                break
            
            all_tabs.extend(tabs)
            print(f"✓ ({len(tabs)} tabs)")
            if len(tabs) < 50:
                break
            
            page += 1
            time.sleep(1)  # Be nice to the server
        
        print(f"\n✓ Total tabs scraped: {len(all_tabs)}")
        return all_tabs
    
    def check_existing_scrape_today(self) -> Optional[str]:
        """
        Check if data has already been scraped today
        
        Returns:
            The timestamp of today's scrape if it exists, None otherwise
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        today = datetime.now().date().isoformat()
        
        cursor.execute('''
            SELECT timestamp
            FROM stats_history
            WHERE date(timestamp) = date(?)
            LIMIT 1
        ''', (today,))
        
        result = cursor.fetchone()
        conn.close()
        
        return result[0] if result else None
    
    def delete_todays_scrape(self):
        """Delete all records from today's scrape"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        today = datetime.now().date().isoformat()
        
        cursor.execute('''
            DELETE FROM stats_history
            WHERE date(timestamp) = date(?)
        ''', (today,))
        
        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()
        
        print(f"✓ Deleted {deleted_count} records from today's scrape")
    
    def save_to_database(self, tabs_data: List[Dict], force: bool = False):
        """
        Save scraped data to database
        
        Args:
            tabs_data: List of tab data dictionaries
            force: If True, skip the daily check (used when user chooses to replace/keep both)
        """
        # Check if already scraped today (unless forced)
        if not force:
            existing_timestamp = self.check_existing_scrape_today()
            
            if existing_timestamp:
                existing_time = datetime.fromisoformat(existing_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                print("\n" + "!" * 80)
                print(f"⚠️  WARNING: Data already scraped today at {existing_time}")
                print("!" * 80)
                print("\nWhat would you like to do?")
                print("  1. Replace old data with new scrape")
                print("  2. Keep both records (not recommended - may affect statistics)")
                print("  3. Stop scraping and keep existing data")
                print()
                
                while True:
                    choice = input("Enter your choice (1/2/3): ").strip()
                    
                    if choice == '1':
                        print("\n🔄 Replacing old data...")
                        self.delete_todays_scrape()
                        break
                    elif choice == '2':
                        print("\n📊 Keeping both records...")
                        break
                    elif choice == '3':
                        print("\n✋ Scraping cancelled. Existing data preserved.")
                        return
                    else:
                        print("Invalid choice. Please enter 1, 2, or 3.")
        
        # Original save logic
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        timestamp = datetime.now().isoformat()
        
        saved_count = 0
        
        for tab in tabs_data:
            # Insert or update tab info
            cursor.execute('''
                INSERT OR REPLACE INTO tabs 
                (tab_id, artist, song_name, tab_type, url, date_submitted, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                tab['tab_id'],
                tab['artist'],
                tab['song_name'],
                tab['tab_type'],
                tab['url'],
                tab['date_submitted'],
                tab['status']
            ))
            
            # Insert stats snapshot
            cursor.execute('''
                INSERT INTO stats_history 
                (tab_id, timestamp, views, rating_stars, rating_count, favorites)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                tab['tab_id'],
                timestamp,
                tab['views'],
                tab['rating_stars'],
                tab['rating_count'],
                tab['favorites']
            ))
            
            saved_count += 1
        
        conn.commit()
        conn.close()
        
        print(f"\n✓ Saved {saved_count} tabs to database")
    
    def get_stats_comparison(self, days: int = 7) -> List[Dict]:
        """Get view/favorite changes over the last N days"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = '''
            WITH latest AS (
                SELECT tab_id, views, favorites, rating_count
                FROM stats_history
                WHERE timestamp = (SELECT MAX(timestamp) FROM stats_history)
            ),
            previous AS (
                SELECT tab_id, views, favorites, rating_count
                FROM stats_history
                WHERE date(timestamp) <= date('now', '-' || ? || ' days')
                ORDER BY timestamp DESC
            )
            SELECT 
                t.artist,
                t.song_name,
                t.tab_type,
                l.views as current_views,
                l.favorites as current_favorites,
                l.rating_count as current_ratings,
                p.views as previous_views,
                p.favorites as previous_favorites,
                p.rating_count as previous_ratings,
                (l.views - COALESCE(p.views, 0)) as views_change,
                (l.favorites - COALESCE(p.favorites, 0)) as favorites_change,
                (l.rating_count - COALESCE(p.rating_count, 0)) as ratings_change
            FROM tabs t
            JOIN latest l ON t.tab_id = l.tab_id
            LEFT JOIN previous p ON t.tab_id = p.tab_id
            ORDER BY views_change DESC
        '''
        
        cursor.execute(query, (days,))
        results = cursor.fetchall()
        conn.close()
        
        return [{
            'artist': r[0],
            'song_name': r[1],
            'tab_type': r[2],
            'current_views': r[3],
            'current_favorites': r[4],
            'current_ratings': r[5],
            'views_change': r[9],
            'favorites_change': r[10],
            'ratings_change': r[11]
        } for r in results]


def main():
    """Example usage"""
    scraper = UGStatsScraper()
    
    # Step 1: Get cookies from your browser
    # You can use a browser extension like "EditThisCookie" or browser dev tools
    # Go to ultimate-guitar.com, open dev tools (F12), go to Application/Storage > Cookies
    # Load cookies
    try:
        with open('ug_cookies.json', 'r') as f:
            cookies = json.load(f)
    except FileNotFoundError:
        print("✗ ug_cookies.json not found. Run cookie_extractor.py first.")
        return
    
    # Step 2: Login
    if not scraper.login(cookies):
        print("Please update the cookies in the script")
        return
    
    # Step 3: Scrape all tabs
    user_id = cookies.get("bbuserid")  # Your user ID
    user_name = cookies.get("bbusername") 
    tabs_data = scraper.scrape_all_pages(user_id, user_name)
    
    # Step 4: Save to database
    scraper.save_to_database(tabs_data)
    
    # Step 5: Show stats comparison (if you have historical data)
    print("\n" + "="*60)
    print("STATS COMPARISON (Last 7 days)")
    print("="*60)
    
    comparison = scraper.get_stats_comparison(days=7)
    for tab in comparison[:10]:  # Top 10
        print(f"\n{tab['artist']} - {tab['song_name']} ({tab['tab_type']})")
        print(f"  Views: {tab['current_views']:,} (+{tab['views_change']:,})")
        print(f"  Favorites: {tab['current_favorites']:,} (+{tab['favorites_change']:,})")


if __name__ == "__main__":
    main()