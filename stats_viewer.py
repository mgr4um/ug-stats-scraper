"""
Stats viewer and automatic scheduler for Ultimate Guitar scraper
"""

import sqlite3
import json
from datetime import datetime, timedelta
from typing import List, Dict
import argparse
import time
import requests


class StatsViewer:
    def __init__(self, db_path: str = "ug_stats.db"):
        self.db_path = db_path
    
    def get_top_tabs(self, metric: str = 'views', limit: int = 10) -> List[Dict]:
        """Get top tabs by a specific metric"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        valid_metrics = ['views', 'favorites', 'rating_count']
        if metric not in valid_metrics:
            metric = 'views'
        
        query = f'''
            SELECT 
                t.artist,
                t.song_name,
                t.tab_type,
                s.{metric}
            FROM tabs t
            JOIN (
                SELECT tab_id, {metric}
                FROM stats_history
                WHERE timestamp = (SELECT MAX(timestamp) FROM stats_history)
            ) s ON t.tab_id = s.tab_id
            ORDER BY s.{metric} DESC
            LIMIT ?
        '''
        
        cursor.execute(query, (limit,))
        results = cursor.fetchall()
        conn.close()
        
        return [{
            'artist': r[0],
            'song_name': r[1],
            'tab_type': r[2],
            metric: r[3]
        } for r in results]
    
    def get_growth_stats(self, days: int = 7) -> Dict:
        """Get overall growth statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Current totals
        cursor.execute('''
            SELECT 
                SUM(views) as total_views,
                SUM(favorites) as total_favorites,
                SUM(rating_count) as total_ratings,
                COUNT(DISTINCT tab_id) as total_tabs
            FROM stats_history
            WHERE timestamp = (SELECT MAX(timestamp) FROM stats_history)
        ''')
        current = cursor.fetchone()
        
        # Previous totals
        cursor.execute('''
            SELECT 
                SUM(views) as total_views,
                SUM(favorites) as total_favorites,
                SUM(rating_count) as total_ratings
            FROM stats_history
            WHERE date(timestamp) <= date('now', '-' || ? || ' days')
            AND timestamp = (
                SELECT MAX(timestamp) 
                FROM stats_history 
                WHERE date(timestamp) <= date('now', '-' || ? || ' days')
            )
        ''', (days, days))
        previous = cursor.fetchone()
        
        conn.close()
        
        if not previous or not previous[0]:
            return {
                'total_tabs': current[3] or 0,
                'total_views': current[0] or 0,
                'total_favorites': current[1] or 0,
                'total_ratings': current[2] or 0,
                'period_days': days,
                'views_change': 0,
                'favorites_change': 0,
                'ratings_change': 0
            }
        
        return {
            'total_tabs': current[3] or 0,
            'total_views': current[0] or 0,
            'total_favorites': current[1] or 0,
            'total_ratings': current[2] or 0,
            'period_days': days,
            'views_change': (current[0] or 0) - (previous[0] or 0),
            'favorites_change': (current[1] or 0) - (previous[1] or 0),
            'ratings_change': (current[2] or 0) - (previous[2] or 0)
        }
    
    def get_trending_tabs(self, days: int = 7, limit: int = 10, sort_by: str = 'views') -> List[Dict]:
        """Get tabs with biggest growth in the period"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Determine sort column
        sort_column = 'views_growth' if sort_by == 'views' else 'favorites_growth'
        
        query = f'''
            WITH latest AS (
                SELECT tab_id, views, favorites
                FROM stats_history
                WHERE timestamp = (SELECT MAX(timestamp) FROM stats_history)
            ),
            previous AS (
                SELECT tab_id, views, favorites
                FROM stats_history
                WHERE date(timestamp) <= date('now', '-' || ? || ' days')
                AND timestamp = (
                    SELECT MAX(timestamp) 
                    FROM stats_history 
                    WHERE date(timestamp) <= date('now', '-' || ? || ' days')
                )
            )
            SELECT 
                t.artist,
                t.song_name,
                t.tab_type,
                l.views as current_views,
                l.favorites as current_favorites,
                (l.views - COALESCE(p.views, 0)) as views_growth,
                (l.favorites - COALESCE(p.favorites, 0)) as favorites_growth
            FROM tabs t
            JOIN latest l ON t.tab_id = l.tab_id
            LEFT JOIN previous p ON t.tab_id = p.tab_id
            WHERE (l.views - COALESCE(p.views, 0)) > 0 
               OR (l.favorites - COALESCE(p.favorites, 0)) > 0
            ORDER BY {sort_column} DESC
            LIMIT ?
        '''
        
        cursor.execute(query, (days, days, limit))
        results = cursor.fetchall()
        conn.close()
        
        return [{
            'artist': r[0],
            'song_name': r[1],
            'tab_type': r[2],
            'current_views': r[3],
            'current_favorites': r[4],
            'views_growth': r[5],
            'favorites_growth': r[6]
        } for r in results]
    
    def get_tab_history(self, artist: str, song_name: str) -> List[Dict]:
        """Get historical data for a specific tab"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = '''
            SELECT 
                sh.timestamp,
                sh.views,
                sh.favorites,
                sh.rating_stars,
                sh.rating_count
            FROM stats_history sh
            JOIN tabs t ON sh.tab_id = t.tab_id
            WHERE LOWER(t.artist) = LOWER(?)
            AND LOWER(t.song_name) = LOWER(?)
            ORDER BY sh.timestamp
        '''
        
        cursor.execute(query, (artist, song_name))
        results = cursor.fetchall()
        conn.close()
        
        return [{
            'timestamp': r[0],
            'views': r[1],
            'favorites': r[2],
            'rating_stars': r[3],
            'rating_count': r[4]
        } for r in results]
    
    def print_dashboard(self, days: int = 7):
        """Print a comprehensive dashboard"""
        print("\n" + "=" * 80)
        print(f"ULTIMATE GUITAR STATS DASHBOARD".center(80))
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".center(80))
        print("=" * 80)
        
        # Overall stats
        growth = self.get_growth_stats(days)
        print(f"\n📊 OVERALL STATS (Last {days} days)")
        print("-" * 80)
        print(f"Total Tabs: {growth['total_tabs']}")
        print(f"Total Views: {growth['total_views']:,} (+{growth['views_change']:,})")
        print(f"Total Favorites: {growth['total_favorites']:,} (+{growth['favorites_change']:,})")
        print(f"Total Ratings: {growth['total_ratings']:,} (+{growth['ratings_change']:,})")
        
        # Top tabs
        print(f"\n🏆 TOP 10 TABS BY VIEWS")
        print("-" * 80)
        top_tabs = self.get_top_tabs('views', 10)
        for i, tab in enumerate(top_tabs, 1):
            print(f"{i:2}. {tab['artist']} - {tab['song_name']} ({tab['tab_type']})")
            print(f"    {tab['views']:,} views")
        
        # Trending tabs
        print(f"\n🔥 TRENDING TABS (Biggest growth in last {days} days)")
        print("-" * 80)
        trending = self.get_trending_tabs(days, 10)
        for i, tab in enumerate(trending, 1):
            print(f"{i:2}. {tab['artist']} - {tab['song_name']} ({tab['tab_type']})")
            print(f"    +{tab['views_growth']:,} views | +{tab['favorites_growth']:,} favorites")
        
        print("\n" + "=" * 80 + "\n")

def send_telegram_alert(message: str):
    BOT_TOKEN = "8596476152:AAHVeCZ83iMnGTishYffgywEKDR_QSiiTNg"
    CHAT_ID = "6606184687"
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        data = {"chat_id": CHAT_ID, "text": message}
        requests.post(url, data=data, timeout=5)
        print("📲 Telegram alert sent")
    except Exception as e:
        print(f"✗ Failed to send Telegram alert: {e}")

def scheduled_scrape():
    """Run the scraper (to be called by scheduler)"""
    import json
    from ug_scraper import UGStatsScraper
    
    # Load cookies
    try:
        with open('ug_cookies.json', 'r') as f:
            cookies = json.load(f)
    except FileNotFoundError:
        print("✗ ug_cookies.json not found. Run cookie_extractor.py first.")
        return
    
    # Run scraper
    scraper = UGStatsScraper()
    
    # Check if already scraped today
    existing_timestamp = scraper.check_existing_scrape_today()
    if existing_timestamp:
        existing_time = datetime.fromisoformat(existing_timestamp).strftime('%Y-%m-%d %H:%M:%S')
        print(f"ℹ️  Already scraped today at {existing_time}. Skipping scheduled scrape.")
        return
    
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        if scraper.login(cookies):
            break
        else:
            print(f"✗ Login failed (attempt {attempt+1}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES - 1:
                print("⏳ Retrying in 10 minutes...")
                time.sleep(600)  # 10 minutes
            else:
                print("🚨 All login attempts failed — cookies likely expired.")
                message = (
                    f"⚠️ Ultimate Guitar login failed after {MAX_RETRIES} attempts.\n"
                    "Cookies may have expired on your Google VM.\n\n"
                    f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                send_telegram_alert(message)
                return
    
    # Scrape data
    user_id = cookies.get('bbuserid')
    user_name = cookies.get('bbusername')
    tabs_data = scraper.scrape_all_pages(int(user_id), user_name)
    
    # Save to database (force=True to skip the interactive prompt in scheduled mode)
    scraper.save_to_database(tabs_data, force=True)
    
    print(f"✓ Scraping completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def setup_scheduler(time_str: str = "00:00"):
    """
    Set up automatic daily scraping
    
    Args:
        time_str: Time in HH:MM format (24-hour)
    """
    try:
        import schedule
        import time
    except ImportError:
        print("✗ 'schedule' library not installed")
        print("  Install with: pip install schedule")
        return
    
    schedule.every().day.at(time_str).do(scheduled_scrape)
    
    print(f"✓ Scheduler set up to run daily at {time_str}")
    print("  Press Ctrl+C to stop")
    print()
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        print("\n✓ Scheduler stopped")


def export_to_csv(output_file: str = "ug_stats_export.csv"):
    """Export current stats to CSV"""
    import csv
    
    conn = sqlite3.connect("ug_stats.db")
    cursor = conn.cursor()
    
    query = '''
        SELECT 
            t.artist,
            t.song_name,
            t.tab_type,
            t.date_submitted,
            t.status,
            t.url,
            s.views,
            s.favorites,
            s.rating_stars,
            s.rating_count,
            s.timestamp
        FROM tabs t
        JOIN stats_history s ON t.tab_id = s.tab_id
        WHERE s.timestamp = (SELECT MAX(timestamp) FROM stats_history)
        ORDER BY s.views DESC
    '''
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Artist', 'Song Name', 'Type', 'Date Submitted', 'Status', 
            'URL', 'Views', 'Favorites', 'Rating Stars', 'Rating Count', 
            'Last Updated'
        ])
        writer.writerows(results)
    
    conn.close()
    print(f"✓ Exported to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Ultimate Guitar Stats Viewer and Scheduler'
    )
    parser.add_argument(
        'command',
        choices=['dashboard', 'top', 'trending', 'history', 'scrape', 'schedule', 'export'],
        help='Command to run'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=7,
        help='Number of days for statistics (default: 7)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=10,
        help='Number of results to show (default: 10)'
    )
    parser.add_argument(
        '--metric',
        choices=['views', 'favorites', 'rating_count'],
        default='views',
        help='Metric for top tabs (default: views)'
    )
    parser.add_argument(
        '--artist',
        type=str,
        help='Artist name for history command'
    )
    parser.add_argument(
        '--song',
        type=str,
        help='Song name for history command'
    )
    parser.add_argument(
        '--time',
        type=str,
        default='00:00',
        help='Time for scheduled scraping in HH:MM format (default: 00:00)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='ug_stats_export.csv',
        help='Output file for export command'
    )
    
    args = parser.parse_args()
    viewer = StatsViewer()
    
    if args.command == 'dashboard':
        viewer.print_dashboard(args.days)
    
    elif args.command == 'top':
        print(f"\n🏆 TOP {args.limit} TABS BY {args.metric.upper()}")
        print("=" * 80)
        tabs = viewer.get_top_tabs(args.metric, args.limit)
        for i, tab in enumerate(tabs, 1):
            print(f"{i:2}. {tab['artist']} - {tab['song_name']} ({tab['tab_type']})")
            print(f"    {tab[args.metric]:,} {args.metric}")
        print()
    
    elif args.command == 'trending':
        metric_name = 'FAVORITES' if args.metric == 'favorites' else 'VIEWS'
        print(f"\n🔥 TOP {args.limit} TRENDING TABS BY {metric_name} (Last {args.days} days)")
        print("=" * 80)
        trending = viewer.get_trending_tabs(args.days, args.limit, sort_by=args.metric if args.metric in ['views', 'favorites'] else 'views')
        for i, tab in enumerate(trending, 1):
            print(f"{i:2}. {tab['artist']} - {tab['song_name']} ({tab['tab_type']})")
            print(f"    Current: {tab['current_views']:,} views | {tab['current_favorites']:,} favorites")
            print(f"    Growth: +{tab['views_growth']:,} views | +{tab['favorites_growth']:,} favorites")
        print()
    
    elif args.command == 'history':
        if not args.artist or not args.song:
            print("✗ --artist and --song are required for history command")
            return
        
        print(f"\n📈 HISTORY: {args.artist} - {args.song}")
        print("=" * 80)
        history = viewer.get_tab_history(args.artist, args.song)
        
        if not history:
            print("No data found for this tab")
            return
        
        for entry in history:
            date = datetime.fromisoformat(entry['timestamp']).strftime('%Y-%m-%d %H:%M')
            rating = f"{entry['rating_stars']:.1f}★" if entry['rating_stars'] else "N/A"
            print(f"{date}: {entry['views']:,} views | {entry['favorites']:,} favorites | "
                  f"{rating} ({entry['rating_count']} votes)")
        print()
    
    elif args.command == 'scrape':
        print("Running scraper...")
        scheduled_scrape()
    
    elif args.command == 'schedule':
        print(f"Setting up automatic scraping at {args.time} daily...")
        setup_scheduler(args.time)
    
    elif args.command == 'export':
        export_to_csv(args.output)


if __name__ == "__main__":
    main()