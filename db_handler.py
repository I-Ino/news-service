import json
import os
import subprocess
import logging
from datetime import datetime, timezone
from pymongo import MongoClient, errors

import config as CONFIG


class DB_Handler:

    def __init__(self):
        self.client = MongoClient(CONFIG.URI)
        self.database = self.client[CONFIG.DB_NAME]
        self.collection = self.database[CONFIG.DB_COLLECTION]

        self.source_file_path = CONFIG.backup_json_path
        self.log_file_path = CONFIG.database_log

        logging.basicConfig(
            filename = self.log_file_path,
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Track last modified time of json
        self.last_modified = None
    

    def load_json(self):

        if not os.path.exists(self.source_file_path):
            raise FileNotFoundError(f"Backup file not found.")
        with open(self.source_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        return data

    def check_for_changes(self):
        # Check is json file has changes

        current_modified = os.path.getmtime(self.source_file_path)
        if self.last_modified is None or current_modified != self.last_modified:
            self.last_modified = current_modified
            return True

        return False

    def sync_db(self, user_id):
        # Sync backup_json with DB, Inserts only new entries based on the unique id.
        
        if not self.check_for_changes():
            print("Database is up to date. \n")
            return

        data = self.load_json()
        new_entries_count = 0

        for uid, entry in data.items():

            # default
            document = {
                "_id": uid,
                "Name": entry.get("Name",""),
                "Type": entry.get("Type",""),
                "URL": entry.get("URL",""),
                "Status": "Not Covered",
                "Notebook_LM": ""
            }       

            try:
                # Insert the document, ignore if duplicate key exists
                
                self.collection.insert_one(document)
                new_entries_count +=1
                log_message = f"{uid} added by {user_id}."
                logging.info(log_message)
            
            except errors.DuplicateKeyError:
                #Skip duplicate article
                continue
        
        if new_entries_count == 0:
            print("Database is up to date.\n")
        else:
            print(f"{new_entries_count} new articles added to the database")


if __name__ == "__main__":
    db_handler = DB_Handler()
    db_handler.sync_db(user_id=CONFIG.user_id)

