import json
import re
import os

import config as CONFIG

class JSON_Parser : 

    def __init__ (self):

        self.source_path = r"C:\Users\Shiv\news-service\resources\seen_articles.json"
        self.destination_path = CONFIG.backup_json_path
        self.last_index = CONFIG.last_index

        self.TYPE_MAP = {
            "General Article" : "genArt",
            "Current Affairs Pointers"  : "cuAff",
            "UPSC Key"        : "uKey",
            "Knowledge Nugget": "knoNugg",
            "Issue at a Glance": "issueGla",
            "Mains Answer Writing": "mainsAns"
        }

        self.VARIABLE_MAP = {
            "General Article" : "general_article_seq",
            "Current Affairs Pointers"  : "current_affair_seq",
            "UPSC Key"        : "upsc_key_seq",
            "Knowledge Nugget": "knowledge_nugget_seq",
            "Issue at a Glance": "issue_glance_seq",
            "Mains Answer Writing": "mains_answer_weekly_seq"
        }

    def variable_update(self, type):
        var_name = self.VARIABLE_MAP[type]
        current = getattr(CONFIG, var_name)
        setattr(CONFIG, var_name, current + 1)
        
    def save_config(self):
        path = CONFIG.__file__

        with open(path, "r", encoding="utf-8") as f:
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
            else:
                new_lines.append(line)

        with open(path, "w", encoding="utf-8") as f:
            f.writelines(new_lines)

        

            
    def UID_Maker(self, rawtype):

        if rawtype.startswith("Mains Answer Writing"):
            rawtype = "Mains Answer Writing"

        self.variable_update(rawtype)

        variable = self.VARIABLE_MAP[rawtype]
        value = getattr(CONFIG, variable)
        unique_id = self.TYPE_MAP[rawtype]+str(value).zfill(4)
        return unique_id


    def generate_new_json(self):

        if not os.path.exists(self.source_path):
            raise FileNotFoundError("Input JSON Not Found!")
                # Add proper error handling

        os.makedirs(os.path.dirname(self.destination_path), exist_ok=True)

        with open(self.source_path, "r", encoding="utf-8") as f:
            data = json.load(f)

                # Empty dict to store new data
            new_data = {}

            for _, article in data.items():
                
                article_type = article["Type"]
                if article_type.startswith("Mains Answer Writing"):
                    article_type = "Mains Answer Writing"

                if article_type not in self.TYPE_MAP:
                    raise ValueError(f"Unknown entry found! {article_type}, at {_} Aborting")
                unique_id = self.UID_Maker(article_type)
                title = article["Title"]
                link = article["Link"]

                new_data[unique_id] = {
                    "Type": article_type,
                    "Name": title,
                    "URL" : link
                }

                print(f"Added {_}.\n")


            with open(self.destination_path,"w", encoding="utf-8") as f:
                json.dump(new_data, f, indent=4, ensure_ascii=False)
                
            self.save_config()
                
if __name__=="__main__":
    jsonParser = JSON_Parser()
    jsonParser.generate_new_json()
