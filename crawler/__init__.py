from utils import get_logger
from collections import Counter
from crawler.frontier import Frontier
from crawler.worker import Worker

class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.workers = list()
        self.worker_factory = worker_factory

        #report statistics
        self.total_word_count = Counter()
        self.longest_page = {}
        self.total_unique_pages = set()
        self.total_sub_domains = []

    def return_report_stats(self):
        num_unique_pages = len(self.total_unique_pages)
        longest_page = self.longest_page[0]
        top_50 = self.total_word_count.most_common(50)
        subdomain_list = self.total_sub_domains.sort()
    
        return num_unique_pages, longest_page, top_50, subdomain_list

    def start_async(self):
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier)
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()

    def start(self):
        self.start_async()
        self.join()

    def join(self):
        for worker in self.workers:
            worker.join()
