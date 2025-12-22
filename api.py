from pydantic import BaseModel
from typing import Optional
from fastapi import FastAPI, HTTPException
from threading import Lock
import logging


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

        self.pipline_runnig = False

        logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
        self.logger = logging.getLogger(__name__)

        self.pipeline_lock = Lock()
        self._register_routes()
        

    def _register_routes(self):

        @self.app.get("/")
        def root():
            return {
                "service": "News Service API",
                "status": "running",
                "version": "1.0.1",
                "endpoints": [
                    "/health",
                    "/pipeline/run"
                ]
            }

        @self.app.get('/health')
        def health_check():
            return{"status":"ok"}
        
        @self.app.get("/pipeline/status")
        def pipeline_status():
            return{
                "running" : self.pipline_runnig
            }

        @self.app.post("/pipeline/run")
        def run_pipeline(request: PipelineRequest):
            
            self.logger.info("Pipeline run requested by %s", request.user_id)

            locked = self.pipeline_lock.acquire(blocking=False)
            if not locked:
                self.logger.warning("Pipeline already running")
                raise HTTPException(
                    status_code=429,
                    detail="Pipeline already running"
                )
            
            self.pipline_runnig = True
            
            response = {
                "status":"success",
                "feed_new_articles":0,
                "json_new_articles":0,
                "db_new_articles":0,
                "errors":[]
                }

            try:    
                self.logger.info("Searching news feed.")
                feed_count = self.feed_tracker.check_feed()
                self.logger.info("Feed Check completed: %s new articles", feed_count)

                self.logger.info("Starting Json generation.")
                json_count = self.json_parser.generate_new_json()
                self.logger.info("%s new articles added", json_count)

                self.logger.info("Starting DB sync")
                db_count = self.db_handler.sync_db(user_id=request.user_id)
                self.logger.info("%s new articles synced", db_count)


                response["feed_new_articles"] = feed_count
                response["json_new_articles"] = json_count or 0
                response["db_new_articles"] = db_count or 0

                self.logger.info(("Pipeline completed successfully"))

                return response
            

            except Exception as e:
                self.logger.exception("Pipeline failed.")
                response["status"] = "failed"
                response["errors"].append(str(e))
                raise HTTPException(status_code=500, detail=response)
            
            finally:
                if locked:
                    self.pipeline_lock.release()
                    self.logger.info("Pipeline lock released")


news_service = NewsService()
app = news_service.app


if __name__=="__main__":
    import uvicorn
    uvicorn.run("api:app", reload=True)