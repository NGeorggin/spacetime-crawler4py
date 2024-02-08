from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker

class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        # Ensure the crawler initializes the correct number of workers, each with a unique worker_id
        self.workers = [Worker(worker_id=i, config=config, frontier=self.frontier) for i in range(4)] 
        self.worker_factory = worker_factory

    def start_async(self):
        self.workers = [
            Worker(worker_id=i, config=self.config, frontier=self.frontier) # modified for multithreads
            for i in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()

    def start(self):
        self.start_async()
        self.join()

    def join(self):
        for worker in self.workers:
            worker.join()
