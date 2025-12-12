import feedparser
import time
import json
import os
import re


import config as CONFIG
from db_handler import DB_Handler


class FeedTracker:

    def __init__(self):
        self.feed_url = CONFIG.feed_url
            # directly storing inside the backup json
        self.destination_file = CONFIG.backup_json_path
        self.new_json = CONFIG.source_json_path
        self.last_index = 0
        self.articles = None
        self.check_interval = 300   # 5 minutes. Make it way larger in final build

        self.db = DB_Handler()
        self.db.built_url_index()

        

    def cleaner(self, title):
        title = title.strip()

         # Skip Daily Subject Wise Quiz (any variant, case-insensitive)
        if re.search(r"Daily Subject[- ]Wise Quiz", title, re.IGNORECASE):
            return None, None

        # UPSC Key
        if re.search(r"UPSC Key", title, re.IGNORECASE):
            cleaned = re.sub(r"UPSC Key", "", title, flags=re.IGNORECASE).strip(" -:|")
            return "UPSC Key", cleaned

        # Issue at a Glance
        if re.search(r"UPSC Issue at a Glance", title, re.IGNORECASE):
            cleaned = re.sub(r"UPSC Issue at a Glance", "", title, flags=re.IGNORECASE).strip(" -:|")
            return "Issue at a Glance", cleaned

        # Knowledge Nugget
        if re.search(r"Knowledge Nugget", title, re.IGNORECASE):
            cleaned = re.sub(r"Knowledge Nugget", "", title, flags=re.IGNORECASE).strip(" -:|")
            return "Knowledge Nugget", cleaned

        # Mains Answer Writing Practice
        if re.search(r"(UPSC Essentials.*Mains Answer.*Practice|UPSC Essentials Mains Answer Practice)", title, re.IGNORECASE):
            week_match = re.search(r"Week (\d+)", title, re.IGNORECASE)
            week_num = week_match.group(1) if week_match else "X"
            cleaned = re.sub(r"(UPSC Essentials.*Mains Answer.*Practice|UPSC Essentials Mains Answer Practice)", "", title, flags=re.IGNORECASE).strip(" -:|")
            return f"Mains Answer Writing - Week {week_num}", cleaned
        
        # Current Affair Pointers (titles containing "Current Affairs Pointers")
        if re.search(r"Current Affairs Pointers", title, re.IGNORECASE):
            date_match = re.search(r"\|\s*(.+)$", title)  # capture everything after '|'
            cleaned = date_match.group(1).strip() if date_match else ""
            return "Current Affairs Pointers", cleaned
        
        # Beyond Trending
        if re.search(r"Beyond Trending", title, re.IGNORECASE):
            cleaned = re.sub(r"Beyond Trending", title, flags=re.IGNORECASE).strip(" -:|")


        # Anything else
        cleaned = title.strip(" -:|")
        return "General Article", cleaned

    def check_feed(self):

        feed = feedparser.parse(self.feed_url)
        new_articles = []

        for entry in feed.entries:
            title = entry.title
            url = entry.url
            
            # Get Article Type & Cleaned Article Name
            article_type, cleaned_title = self.cleaner(title)
        
            # Skip Daily Subject Wise Quizes
            if article_type is None:
                continue

            # check if URL already exist in the database
            if self.db.is_duplicate_url(url):
                continue

            # if not a Quiz or a duplicate enter it to json
            self.last_index += 1
            article_id = str(self.last_index)
            
            new_articles[article_id] = {
                "Type": article_type,
                "Name": cleaned_title,
                "URL": url
            }
        
        if not new_articles:
            print("No new articles found")
            return
        
        os.makedirs(os.path.dirname(self.new_json),exist_ok=True)

        with open(self.destination_file, "w", encoding="utf-8") as f:
            json.dump(new_articles, f, indent=4, ensure_ascii=False)

        print (f"Saved {len(new_articles)} new unique articles.")
        

if __name__ == "__main__":
    tracker = FeedTracker()
    tracker.check_feed()