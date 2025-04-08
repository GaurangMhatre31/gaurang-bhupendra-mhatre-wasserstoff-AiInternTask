# All imports remain the same
import os
import json
import time
import random
import logging
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict

import playwright.sync_api as pw
from dotenv import load_dotenv
from fake_useragent import UserAgent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("linkedin_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class LinkedInProfile:
    name: str
    profile_url: str
    headline: Optional[str] = None
    location: Optional[str] = None
    scraped_at: str = datetime.now().isoformat()

class LinkedInCache:
    def __init__(self, cache_file: str = "linkedin_cache.json"):
        self.cache_file = cache_file
        self.cache = self._load_cache()

    def _load_cache(self) -> Dict[str, Any]:
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning(f"Cache file {self.cache_file} is corrupted. Creating new cache.")
        return {"profiles": {}, "visited_search_pages": [], "cookies": None, "last_session": None}

    def save_cache(self) -> None:
        with open(self.cache_file, 'w') as f:
            json.dump(self.cache, f, indent=2)

    def add_profile(self, profile: LinkedInProfile) -> None:
        self.cache["profiles"][profile.profile_url] = asdict(profile)
        self.save_cache()

    def profile_exists(self, profile_url: str) -> bool:
        return profile_url in self.cache["profiles"]

    def add_visited_search_page(self, search_url: str) -> None:
        if search_url not in self.cache["visited_search_pages"]:
            self.cache["visited_search_pages"].append(search_url)
            self.save_cache()

    def is_search_page_visited(self, search_url: str) -> bool:
        return search_url in self.cache["visited_search_pages"]

    def save_cookies(self, cookies: List[Dict[str, Any]]) -> None:
        self.cache["cookies"] = cookies
        self.cache["last_session"] = datetime.now().isoformat()
        self.save_cache()

    def get_cookies(self) -> Optional[List[Dict[str, Any]]]:
        if not self.cache.get("cookies") or not self.cache.get("last_session"):
            return None
        last_session = datetime.fromisoformat(self.cache["last_session"])
        if datetime.now() - last_session > timedelta(hours=12):
            logger.info("Cached session expired. Need to login again.")
            return None
        return self.cache["cookies"]

class LinkedInScraper:
    def __init__(
        self,
        email: str,
        password: str,
        headless: bool = True,
        cache_file: str = "linkedin_cache.json",
        max_profiles: int = 200,
        random_delay_min: float = 2.5,
        random_delay_max: float = 6.5
    ):
        self.email = email
        self.password = password
        self.headless = headless
        self.cache = LinkedInCache(cache_file)
        self.max_profiles = max_profiles
        self.random_delay_min = random_delay_min
        self.random_delay_max = random_delay_max

        self.user_agent = UserAgent().random
        self.browser = None
        self.context = None
        self.page = None

    def random_delay(self) -> None:
        time.sleep(random.uniform(self.random_delay_min, self.random_delay_max))

    def start_browser(self) -> bool:
        try:
            logger.info("Starting browser...")
            self.pw_instance = pw.sync_playwright().start()
            self.browser = self.pw_instance.chromium.launch(headless=self.headless)
            self.context = self.browser.new_context(user_agent=self.user_agent)
            self.page = self.context.new_page()
            return True
        except Exception as e:
            logger.error(f"Error starting browser: {str(e)}")
            return False

    def close_browser(self) -> None:
        try:
            if self.context:
                cookies = self.context.cookies()
                self.cache.save_cookies(cookies)
                logger.info("Session cookies saved.")
            if self.browser:
                self.browser.close()
            if self.pw_instance:
                self.pw_instance.stop()
            logger.info("Browser closed.")
        except Exception as e:
            logger.error(f"Error closing browser: {str(e)}")

    def login(self) -> bool:
        try:
            logger.info("Logging in to LinkedIn...")
            self.page.goto("https://www.linkedin.com/login")
            self.page.wait_for_selector('input[id="username"]')
            self.random_delay()
            self.page.fill('input[id="username"]', self.email)
            self.page.fill('input[id="password"]', self.password)
            self.page.click('button[type="submit"]')
            self.page.wait_for_load_state('networkidle')
            logger.info("Login successful.")
            return True
        except Exception as e:
            logger.error(f"Login failed: {str(e)}")
            return False

    def search_profiles(self, search_query: str) -> bool:
        for attempt in range(3):
            try:
                logger.info(f"Searching for: {search_query}")
                search_url = f"https://www.linkedin.com/search/results/people/?keywords={search_query.replace(' ', '%20')}"
                self.page.goto(search_url, timeout=30000)
                self.page.wait_for_load_state("networkidle")
                self.page.wait_for_timeout(5000)

                # Handle checkpoint or redirect
                if "checkpoint" in self.page.url:
                    logger.warning("Checkpoint or redirect page detected. Manual intervention may be required.")
                    return False

                self.page.wait_for_selector('div.reusable-search__result-container', timeout=30000)

                if self.cache.is_search_page_visited(self.page.url):
                    logger.info("Search page already visited. Skipping.")
                    return True

                self.cache.add_visited_search_page(self.page.url)

                profiles = self.page.query_selector_all('li.reusable-search__result-container')
                count = 0

                for profile_elem in profiles:
                    try:
                        name_elem = profile_elem.query_selector('span.entity-result__title-text a span[aria-hidden="true"]')
                        profile_link_elem = profile_elem.query_selector('a.app-aware-link')
                        headline_elem = profile_elem.query_selector('div.entity-result__primary-subtitle')
                        location_elem = profile_elem.query_selector('div.entity-result__secondary-subtitle')

                        name = name_elem.inner_text().strip() if name_elem else "N/A"
                        profile_url = profile_link_elem.get_attribute('href').split('?')[0] if profile_link_elem else "N/A"
                        headline = headline_elem.inner_text().strip() if headline_elem else None
                        location = location_elem.inner_text().strip() if location_elem else None

                        if profile_url and not self.cache.profile_exists(profile_url):
                            profile_data = LinkedInProfile(
                                name=name,
                                profile_url=profile_url,
                                headline=headline,
                                location=location
                            )
                            self.cache.add_profile(profile_data)
                            logger.info(f"Profile scraped: {name} - {profile_url}")
                            count += 1
                            if count >= self.max_profiles:
                                logger.info("Reached max profile limit.")
                                break
                        self.random_delay()
                    except Exception as e:
                        logger.warning(f"Failed to parse a profile: {str(e)}")
                return True
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed: {str(e)}")
                self.random_delay()
        logger.error("All attempts to load search results failed.")
        return False

def main():
    parser = argparse.ArgumentParser(description="LinkedIn Profile Scraper")
    parser.add_argument("--email", required=True, help="LinkedIn Email")
    parser.add_argument("--password", required=True, help="LinkedIn Password")
    parser.add_argument("--query", required=True, help="Search Query")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    parser.add_argument("--max", type=int, default=200, help="Maximum profiles to scrape")

    args = parser.parse_args()

    scraper = LinkedInScraper(
        email=args.email,
        password=args.password,
        headless=args.headless,
        max_profiles=args.max
    )

    if scraper.start_browser():
        if scraper.login():
            scraper.search_profiles(args.query)
        scraper.close_browser()

if __name__ == "__main__":
    main()
