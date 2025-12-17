import json
import re
import os

import config as CONFIG

class JSON_Parser : 

    def __init__ (self):

        self.source_path = CONFIG.source_json_path
        self.destination_path = CONFIG.backup_json_path

        self.TYPE_MAP = {
            "General Article" : "genArt",
            "Current Affairs Pointers"  : "cuAff",
            "UPSC Key"        : "uKey",
            "Knowledge Nugget": "knoNugg",
            "Issue at a Glance": "issueGla",
            "Mains Answer Writing": "mainsAns",
            "Beyond Trending": "beyTre",
            "UPSC Interview Special": "interview",
            "The world this week": "worldWeek"
        }

        self.VARIABLE_MAP = {
            "General Article" : "general_article_seq",
            "Current Affairs Pointers"  : "current_affair_seq",
            "UPSC Key"        : "upsc_key_seq",
            "Knowledge Nugget": "knowledge_nugget_seq",
            "Issue at a Glance": "issue_glance_seq",
            "Mains Answer Writing": "mains_answer_weekly_seq",
            "Beyond Trending": "beyond_trending_seq",
            "UPSC Interview Special": "interview_seq",
            "The world this week": "world_this_week_seq"
        }

    def variable_update(self, type):
        var_name = self.VARIABLE_MAP[type]
        current = getattr(CONFIG, var_name)
        setattr(CONFIG, var_name, current + 1)

    def should_skip(self, title):
        return "upsc weekly current affairs quiz" in title.lower()

    def normalize_type(self, article_type, title):
        if "UPSC Interview Special" in title:
            return "UPSC Interview Special"
        if title.lower().startswith("the world this week"):
            return "The world this week"
        return article_type


    def clean_title(self, title):
        title = re.sub(r"\s*\|\s*.*$", "", title)   # remove text after "|"
        title = re.sub(r"\s*:\s*", " - ", title)    # normalize colon
        return title.strip()

        
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
            elif line.startswith("beyond_trending_seq"):
                new_lines.append(f"beyond_trending_seq = {CONFIG.beyond_trending_seq}\n")
            elif line.startswith("interview_seq"):
                new_lines.append(f"interview_seq = {CONFIG.interview_seq}\n")
            elif line.startswith("world_this_week_seq"):
                new_lines.append(f"world_this_week_seq = {CONFIG.world_this_week_seq}\n")
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

                # Load existing backup JSON
            if os.path.exists(self.destination_path):
                with open(self.destination_path, "r", encoding="utf-8") as backup:
                    new_data = json.load(backup)
            else:
                new_data = {}

                # Reverse index of existing URLs to ensure no duplicate entry 
            existing_url = {entry["URL"] for entry in new_data.values()}
            

                # Counter for new articles added:
            article_counter = 0
            

            for _, article in data.items():
                if self.should_skip(article["Name"]):
                    continue
                article_type = self.normalize_type(article["Type"], article["Name"])
                if article_type.startswith("Mains Answer Writing"):
                    article_type = "Mains Answer Writing"

                if article_type not in self.TYPE_MAP:
                    raise ValueError(f"Unknown entry found! {article_type}, at {_}. Aborting")
                

                
                title = self.clean_title(article["Name"])
                link = article["URL"]

                # Skipping enty if url already present.
                if link in existing_url:
                        # messages look ugly
                    #print(f"Skipping duplicate: {title} at {_}.\n\n")
                    continue

                    # Checking UID after duplicates are checked
                unique_id = self.UID_Maker(article_type)

                new_data[unique_id] = {
                    "Type": article_type,
                    "Name": title,
                    "URL" : link
                }

                existing_url.add(link)  # maintain the index
                article_counter += 1
            
            if article_counter == 0:
                print("0 articles found")
                return
            
            print(f"Added {article_counter} articles in the JSON. \n")


            with open(self.destination_path,"w", encoding="utf-8") as f:
                json.dump(new_data, f, indent=4, ensure_ascii=False)
                
            self.save_config()
                
if __name__=="__main__":
    jsonParser = JSON_Parser()
    jsonParser.generate_new_json()
