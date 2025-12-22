import json
import os
import logging
from pymongo import MongoClient, errors

import config as CONFIG


class DB_Handler:

    def __init__(self):
        self.client = MongoClient(CONFIG.URI)
        self.database = self.client[CONFIG.DB_NAME]
        self.collection = self.database[CONFIG.DB_COLLECTION]

        self.backup_json_path = CONFIG.backup_json_path
        self.log_file_path = CONFIG.database_log

        logging.basicConfig(
            filename = self.log_file_path,
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Track last modified time of json
        self.last_modified = None

        # Reverse index for URL
        self.url_index = {}


        self.TYPE_TO_CONFIG = {
            "General Article": "general_article_seq",
            "UPSC Key": "upsc_key_seq",
            "Knowledge Nugget": "knowledge_nugget_seq",
            "Issue at a Glance": "issue_glance_seq",
            "Mains Answer Weekly": "mains_answer_weekly_seq",
            "Current Affair": "current_affair_seq",
            "Beyond Trending": "beyond_trending_seq",
            "World This Week": "world_this_week_seq",
            "Interview": "interview_seq",
        }


    def built_url_index(self):
        """
        Loads all URLs from MongoDB and builds a lookup for O(1) duplicate detection.
        """
        cursor = self.collection.find({}, {"URL":1})

        self.url_index={}
        for doc in cursor:
            url = doc.get("URL")
            if url:
                # Set Membership
                self.url_index[url] = True
        
        return self.url_index
    

    def is_duplicate_url(self, url: str) -> bool:
        # O(1) URL duplicate detection using the in-memory dictionary

        return url in self.url_index
    

    def load_json(self):

        if not os.path.exists(self.backup_json_path):
            raise FileNotFoundError(f"Backup file not found.")
        with open(self.backup_json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        return data

    def check_for_changes(self):
        # Check is json file has changes

        current_modified = os.path.getmtime(self.backup_json_path)
        if self.last_modified is None or current_modified != self.last_modified:
            self.last_modified = current_modified
            return True

        return False

    def sync_db(self, user_id):
        # Inserts only new entries based on the unique id. Syncs db with backup json

        if not os.path.exists(self.backup_json_path):
            print(f"JSON file not found: {self.backup_json_path}")
            return 0
        
        
        if not self.check_for_changes():
            print("Database is up to date. \n")
            return 0

        data = self.load_json() 
        all_ids_in_json = set(data.keys())

        new_entries_count = 0

        # Fetch all existing ID from MongoDB 
        existing_id = set(doc["_id"] for doc in self.collection.find({},{"_id":1}))

        # Finding new id
        new_ids = all_ids_in_json - existing_id

        self.built_url_index()

        if not new_ids:
            message = "Checked for updates. None found. Database is up to date."
            logging.info(message)
            print(message)
            return 0


        for uid in new_ids:

            entry = data[uid]
            raw_type = entry.get("Type", "")
            article_type = CONFIG.TYPE_NORMALIZATION_MAP.get(raw_type, raw_type)
            # skip if url is duplicate
            url = entry.get("URL","")
            if self.is_duplicate_url(url):
                logging.info(f"Duplicate URL skiped for {uid}: {url}")
                continue


            # Prepare db entry only if not duplicate
            document = {
                "_id": uid,
                "Name": entry.get("Name",""),
                "Type": article_type,
                "URL": url,
                "Status": "Not Covered",
                "Notebook_LM": ""
            }    
               

            try:
                # Insert the document
                
                self.collection.insert_one(document)
                new_entries_count +=1
                log_message = f"{uid} added by {user_id}."
                logging.info(log_message)

                if article_type in self.TYPE_TO_CONFIG:
                    var_name = self.TYPE_TO_CONFIG[article_type]
                    setattr(CONFIG, var_name, getattr(CONFIG, var_name)+1)

                # update url-index
                if url:
                    self.url_index[url] = True
            
            except errors.DuplicateKeyError:
                #Skip duplicate article
                continue
        
        # Persist updated counters to config.py
        config_path = CONFIG.__file__

        with open(config_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        new_lines = []
        for line in lines:
            updated = False
            for var in self.TYPE_TO_CONFIG.values():
                if line.startswith(var):
                    new_lines.append(f"{var} = {getattr(CONFIG, var)}\n")
                    updated = True
                    break
            if not updated:
                new_lines.append(line)

        with open(config_path, "w", encoding="utf-8") as f:
            f.writelines(new_lines)


        if new_entries_count == 0:
            print("Database is up to date.\n")
            return 0
        else:
            print(f"{new_entries_count} new articles added to the database")
            return new_entries_count

    
    def update_notebook_lm_link(self, url:str, NotebookLink: str, user_id: str):
        # Adds Notebook LM into the database

        if not url or not NotebookLink:
            raise ValueError("URL and Notebook Link are required.")
        
        result = self.collection.update_one(
            {"URL": url},
            {
                "$set":{
                    "Notebook_LM": NotebookLink
                }
            }
        )

        if result.matched_count == 0:
            logging.warning(f"No article found for URL: {url}")
            print("No matching article found. Notebook LM link not updated.")
            return

        if result.modified_count == 1:
            logging.info(f"Notebook LM updated by {user_id} for URL: {url}")
            print("Notebook LM link updated successfully.")
        else:
            print("Notebook LM link already up to date.")


    def rebuild_db_from_backup_json(self, user_id: str, clear_db: bool = False):
        """
        Loads entire backup_json into MongoDB and updates config counters.
        No normalization is applied.
        """

        if not os.path.exists(self.backup_json_path):
            raise FileNotFoundError("Backup JSON not found")

        # Load backup JSON
        with open(self.backup_json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            raise ValueError("Invalid backup JSON structure")

        # Optionally clear DB
        if clear_db:
            self.collection.delete_many({})
            logging.info("Database cleared before rebuild")

        # Reset config counters
        CONFIG.general_article_seq = 0
        CONFIG.upsc_key_seq = 0
        CONFIG.knowledge_nugget_seq = 0
        CONFIG.issue_glance_seq = 0
        CONFIG.mains_answer_weekly_seq = 0
        CONFIG.current_affair_seq = 0
        CONFIG.beyond_trending_seq = 0
        CONFIG.world_this_week_seq = 0
        CONFIG.interview_seq = 0

        TYPE_TO_CONFIG = self.TYPE_TO_CONFIG
        processed = 0

        for uid, entry in data.items():
            raw_type = entry.get("Type", "")
            article_type = CONFIG.TYPE_NORMALIZATION_MAP.get(raw_type, raw_type)
            url = entry.get("URL", "")

            if not article_type or not url:
                continue

            try:
                self.collection.update_one(
                    {"_id": uid},
                    {
                        "$setOnInsert": {
                            "_id": uid,
                            "Name": entry.get("Name", ""),
                            "Type": article_type,
                            "URL": url,
                            "Status": "Not Covered",
                            "Notebook_LM": "",
                            "Inserted_By": user_id,
                        }
                    },
                    upsert=True
                )
            except Exception as e:
                logging.error(f"Insert failed for {uid}: {e}")
                continue

            if article_type in TYPE_TO_CONFIG:
                var = TYPE_TO_CONFIG[article_type]
                setattr(CONFIG, var, getattr(CONFIG, var) + 1)

            processed += 1

        # Write updated counters back to config.py
        config_path = CONFIG.__file__
        with open(config_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        new_lines = []
        for line in lines:
            if line.startswith("general_article_seq"):
                new_lines.append(f"general_article_seq = {CONFIG.general_article_seq}\n")
            elif line.startswith("upsc_key_seq"):
                new_lines.append(f"upsc_key_seq = {CONFIG.upsc_key_seq}\n")
            elif line.startswith("knowledge_nugget_seq"):
                new_lines.append(f"knowledge_nugget_seq = {CONFIG.knowledge_nugget_seq}\n")
            elif line.startswith("issue_glance_seq"):
                new_lines.append(f"issue_glance_seq = {CONFIG.issue_glance_seq}\n")
            elif line.startswith("mains_answer_weekly_seq"):
                new_lines.append(f"mains_answer_weekly_seq = {CONFIG.mains_answer_weekly_seq}\n")
            elif line.startswith("current_affair_seq"):
                new_lines.append(f"current_affair_seq = {CONFIG.current_affair_seq}\n")
            elif line.startswith("beyond_trending_seq"):
                new_lines.append(f"beyond_trending_seq = {CONFIG.beyond_trending_seq}\n")
            elif line.startswith("world_this_week_seq"):
                new_lines.append(f"world_this_week_seq = {CONFIG.world_this_week_seq}\n")
            elif line.startswith("interview_seq"):
                new_lines.append(f"interview_seq = {CONFIG.interview_seq}\n")
            else:
                new_lines.append(line)

        with open(config_path, "w", encoding="utf-8") as f:
            f.writelines(new_lines)

        logging.info(f"Rebuild complete. {processed} articles processed.")

        return {
            "articles_processed": processed,
            "general_article": CONFIG.general_article_seq,
            "upsc_key": CONFIG.upsc_key_seq,
            "knowledge_nugget": CONFIG.knowledge_nugget_seq,
            "issue_glance": CONFIG.issue_glance_seq,
            "mains_answer_weekly": CONFIG.mains_answer_weekly_seq,
            "current_affair": CONFIG.current_affair_seq,
            "beyond_trending": CONFIG.beyond_trending_seq,
            "world_this_week": CONFIG.world_this_week_seq,
            "interview": CONFIG.interview_seq,
        }






if __name__ == "__main__":
    db_handler = DB_Handler()
    #db_handler.sync_db(user_id=CONFIG.user_id)
    db_handler.rebuild_db_from_backup_json(user_id=CONFIG.user_id, clear_db=True)
