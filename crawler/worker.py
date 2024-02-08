from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time

from threading import Lock


class Worker(Thread):
    url_dicts = [set()for _ in range(4)] # Assuming we have 4 workers
    dict_locks = [Lock() for _ in range(4)] # Ensure that only one thread can modify the data at a time

    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.worker_id = worker_id # Initialize worker_id
        self.config = config
        self.frontier = frontier

        self.my_urls = Worker.url_dicts[worker_id] # Each worker has direct access to its own set for tracking URL

        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)

    @staticmethod
    def is_url_unique(worker_id, url): # This function is used to check if a URL is unique  across all workers' dictionaries.
        for i, url_dict in enumerate(Worker.url_dicts):
            if i != worker_id:
                with Worker.dict_locks[i]:
                    if url in url_dict:
                        return False
        
        # URL not found in other workers' sets, add to this worker's set
        with Worker.dict_locks[worker_id]:
            Worker.url_dicts[worker_id].add(url)
        return True

        
        
    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped_urls = scraper.scraper(tbd_url, resp)
            for scraped_url in scraped_urls:
                # Check if the URL is unique across all workers before adding
                if Worker.is_url_unique(self.worker_id, scraped_url):
                    self.frontier.add_url(scraped_url)
                else:
                    self.logger.info(f"URL skipped: {scraped_url}")
                    
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)
