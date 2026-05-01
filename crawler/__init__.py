from utils import get_logger
from collections import Counter, defaultdict
from crawler.frontier import Frontier
from crawler.worker import Worker
from threading import Lock

class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.workers = list()
        self.worker_factory = worker_factory

        self.statsLock = Lock()
        self.domain_timer = defaultdict(float)

        #report statistics
        self.total_word_count = Counter()
        self.longest_page = {"url": "", "word_count": 0}
        self.total_unique_pages = set()
        self.total_sub_domains = defaultdict(set)

    def update_stats(self, report_stats):
        # Err if missing or extra stat field
        if len(report_stats) != 4:
            print("Report stats broken")
            return

        #Unpack tuple stored from scraper()
        curr_unique_pages, curr_longest_page, curr_word_count, curr_subdomains = report_stats
        with self.statsLock:
            #Update total unique page set
            self.total_unique_pages.update(curr_unique_pages)

            #Update longest page if longer
            if self.longest_page["word_count"] < curr_longest_page["word_count"]:
                self.longest_page["url"] = curr_longest_page["url"]
                self.longest_page["word_count"] = curr_longest_page["word_count"]

            #Update total word freq Counter
            self.total_word_count.update(curr_word_count)

            #Update the subdomain list
            for subdomain, pages in curr_subdomains.items():
                self.total_sub_domains[subdomain].update(pages)



    def return_report_stats(self):
        with self.statsLock:
            #Takes len of set containing all unique pages found while crawling
            num_unique_pages = len(self.total_unique_pages)

            #Retrieves page name of current longest stored page
            longest_page = self.longest_page["url"]

            #Takes top 50 most common words (pre-filtered in scraper)
            top_50 = self.total_word_count.most_common(50)

            #Takes len of list of unique pages stored in the subdomain's corresponding set, creates a tuple and adds it to a list
            subdomain_list = sorted([(subdomains, len(pages)) for subdomains, pages in self.total_sub_domains.items()])
    
        return num_unique_pages, longest_page, top_50, subdomain_list

    def start_async(self):
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier, self)
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()

    def start(self):
        self.start_async()
        self.join()

    def join(self):
        for worker in self.workers:
            worker.join()
