from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time

from urllib.parse import urlsplit

class Worker(Thread):
    def __init__(self, worker_id, config, frontier, crawler):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.id = worker_id
        self.config = config
        self.frontier = frontier
        self.crawler = crawler
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url(self.id)
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break

            #Politeness check: added by Luke
            parsed = urlsplit(tbd_url)

            if not parsed.hostname:
                continue

            #Retrieve proper domain
            domain = parsed.hostname.lower()
            if (domain.endswith("ics.uci.edu")):
                domain = "ics.uci.edu"
            elif (domain.endswith("cs.uci.edu")):
                domain = "cs.uci.edu"
            elif (domain.endswith("informatics.uci.edu")):
                domain = "ics.uci.edu"
            elif (domain.endswith("stat.uci.edu")):
                domain = "ics.uci.edu"

            with self.crawler.timerLock:

                #Get current time and last time we crawled a new link
                now = time.monotonic()
                last = self.crawler.domain_timer.get(domain, 0)

                #Wait = politeness delay - time diff between now and last download
                wait = self.config.time_delay - (now - last)

                #Update crawl time, if theres a wait we add wait for predicted crawl time
                if wait > 0:
                    self.crawler.domain_timer[domain] = now + wait
                else:
                    self.crawler.domain_timer[domain] = now

            #Sleep if there is wait 
            if wait > 0:
                time.sleep(wait)

            #Download
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            
            #For report
            scraped_urls, report_stats = scraper.scraper(tbd_url, resp)
            self.crawler.update_stats(report_stats)

            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
