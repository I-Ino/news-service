from pydantic import BaseModel
from typing import Optional
from fastapi import FastAPI, HTTPException
from threading import Lock

from utility.feed_parser import FeedTracker
from utility.json_parser import JSON_Parser
from utility.db_handler import DB_Handler

import config as CONFIG


class PipelineRequest(BaseModel):
    user_id: Optional[str] = CONFIG.user_id

class UpdateRequest(BaseModel):
    url:str
    notebook_link: str
    user_id: Optional[str] = CONFIG.user_id


# Core API Class

class NewsService:

    def __init__(self):
        self.app = FastAPI(
            title="News Service API",
            version="1.0.1",
            description="API for UPSC News studying"
        )

        self.feed_tracker = FeedTracker()
        self.json_parser = JSON_Parser()
        self.db_handler = DB_Handler()

        self.pipeline_lock = Lock()
        self._register_routes()
        

    def _register_routes(self):

        @self.app.get('/health')
        def health_check():
            return{"status":"ok"}
        
        @self.app.post("/pipeline/run")
        def run_pipeline(request: PipelineRequest):
            
            locked = self.pipeline_lock.acquire(blocking=False)
            if not locked:
                raise HTTPException(
                    status_code=429,
                    detail="Pipeline already running"
                )
            
            response = {
                "status":"success",
                "feed_new_articles":0,
                "json_new_articles":0,
                "db_new_articles":0,
                "errors":[]
                }

            try:    
                
                feed_count = self.feed_tracker.check_feed()
                json_count = self.json_parser.generate_new_json()
                db_count = self.db_handler.sync_db(user_id=request.user_id)

                response["feed_new_articles"] = feed_count
                response["json_new_articles"] = json_count or 0
                response["db_new_articles"] = db_count or 0

                return response
            
            except HTTPException:
                raise

            except Exception as e:
                response["status"] = "failed"
                response["errors"].append(str(e))
                raise HTTPException(status_code=500, detail=response)
            
            finally:
                if locked:
                    self.pipeline_lock.release()


if __name__=="__main__":
    news_service = NewsService()
    app = news_service.app