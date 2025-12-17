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
        self.source_json_path = CONFIG.source_json_path

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

    def sync_db(self, user_id, json_file_path = None):
        # Sync backup_json with DB, Inserts only new entries based on the unique id.

        # use provided path or fall back
        json_file_path = json_file_path or self.backup_json_path

        if not os.path.exists(json_file_path):
            print(f"JSON file not found: {json_file_path}")
            return
        
        if json_file_path == self.backup_json_path:
            if not self.check_for_changes():
                print("Database is up to date. \n")
                return

        data = self.load_json() if json_file_path == self.backup_json_path else json.load(open(json_file_path, "r", encoding="utf-8"))
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
            return


        for uid in new_ids:

            entry = data[uid]
            # skip if url is duplicate
            url = entry.get("URL","")
            if self.is_duplicate_url(url):
                logging.info(f"Duplicate URL skiped for {uid}: {url}")
                continue


            # Prepare db entry only if not duplicate
            document = {
                "_id": uid,
                "Name": entry.get("Name",""),
                "Type": entry.get("Type",""),
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

                # update url-index
                if url:
                    self.url_index[url] = True
            
            except errors.DuplicateKeyError:
                #Skip duplicate article
                continue
        
        if new_entries_count == 0:
            print("Database is up to date.\n")
        else:
            print(f"{new_entries_count} new articles added to the database")

        # Delete JSON
        try:
            os.remove(json_file_path)
            print(f"Deleted JSON file: {json_file_path}")
        except Exception as e:
            print(f"Failed to delete JSON file: {e}")


    def sync_from_json_and_cleanup(self, user_id):
        """
        Inserts new articles from a JSON file into MongoDB and deletes the JSON after processing.
        Only inserts articles with URLs not already in the database.
        """
        if not os.path.exists(self.source_json_path):
            print(f"No new articles file found at {self.source_json_path}. Nothing to sync.")
            return

        # Load the JSON
        with open(self.source_json_path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                print(f"Error decoding JSON file {self.source_json_path}. Skipping sync.")
                return

        if not data:
            print("No new articles in the JSON file.")
            return

        # Ensure URL index is up to date
        self.built_url_index()

        inserted_count = 0

        for uid, entry in data.items():
            url = entry.get("URL", "")
            if not url or self.is_duplicate_url(url):
                logging.info(f"Skipping duplicate or invalid URL for {uid}: {url}")
                continue

            document = {
                "_id": uid,
                "Name": entry.get("Name", ""),
                "Type": entry.get("Type", ""),
                "URL": url,
                "Status": "Not Covered",
                "Notebook_LM": ""
            }

            try:
                self.collection.insert_one(document)
                inserted_count += 1
                logging.info(f"{uid} added by {user_id} from JSON file.")

                # Update URL index
                self.url_index[url] = True

            except errors.DuplicateKeyError:
                continue

        print(f"Inserted {inserted_count} new articles from JSON file.")

        # Delete JSON after successful sync
        

    
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





if __name__ == "__main__":
    db_handler = DB_Handler()
    db_handler.sync_db(user_id=CONFIG.user_id, json_file_path=CONFIG.source_json_path)

