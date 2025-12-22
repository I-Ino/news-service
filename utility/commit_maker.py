import os
import json
import hashlib
import subprocess
import time
from typing import Set, Dict

import config as CONFIG

class FileTracker:
    # Track file state using hash comparision

    def __init__(self, file_path:str):
        self.file_path = file_path
        self.last_hash = self._compute_hash()

    def _compute_hash(self) -> str:
        if not os.path.exists(self.file_path):
            return ""

        with open (self.file_path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()

    def has_changed(self) -> bool:
        current_hash = self._compute_hash()
        if current_hash != self.last_hash:
            self.last_hash = current_hash
            return True
        return False




class JSONChangeDetector:
    # Detects newly added unique id in json file

    def __init__(self, json_path: str):
        self.json_path = json_path
        self.previous_ids : Set[str] = self._load_ids()

    def _load_ids(self) -> Set[str]:
        if not os.path.exists(self.json_path):
            return set()
        with open(self.json_path, "r", encoding="utf-8") as f:
            data: Dict = json.load(f)

            if not isinstance(data, dict):
                raise ValueError("Expected dict from JSON backup")

        return set(data.keys())
    
    def detect_new_ids(self) -> Set[str]:
        current_ids = self._load_ids()
        new_ids = current_ids - self.previous_ids
        self.previous_ids = current_ids
        return new_ids
    


class GitHandler:

    def __init__(self, repo_path):
        self.repo_path = repo_path

    def _run(self, command: list[str]):
        subprocess.run(
            command,
            cwd=self.repo_path,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False
        )

    def stage_files(self, files: list[str]):
        self._run(["git", "add", *files])

    def commit(self, message: str):
        self._run(["git", "commit", "-m", message])



class CommitMaker:

    def __init__(self):
        self.repo_path = CONFIG.backup_json_path
        self.json_detector = JSONChangeDetector(CONFIG.backup_json_path)
        self.git = GitHandler(self.repo_path)
    
    def commit_if_needed(self):
        if not self._has_git_changes():
            return None
        
        new_ids = self.json_detector.detect_new_ids()
        commit_msg = self._build_commit_message(new_ids)

        self.git.stage_files([
            CONFIG.backup_json_path,
            CONFIG.database_log
        ])
        self.git.commit(commit_msg)
        return commit_msg
    
    def _has_git_changes(self) -> bool:
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=self.repo_path,
            stdout=subprocess.PIPE,
            text=True
        )
        return bool(result.stdout.strip())
    
    @staticmethod
    def _build_commit_message(new_ids: Set[str]) -> str:
        if not new_ids:
            return "Update article backup and database log"
        
        sorted_ids = ", ".join(sorted(new_ids))
        return f"Added : {sorted_ids}"
    


if __name__ == "__main__":
    committer = CommitMaker()
    committer.run()