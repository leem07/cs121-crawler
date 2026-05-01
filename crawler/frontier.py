import os
import shelve

from threading import Thread, RLock
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

from urllib.parse import urlsplit
from collections import defaultdict, deque
from scraper import is_valid
from threading import Lock

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = list()
        self.tbd_worker = defaultdict(deque)

        self.tbdLock = Lock()
        self.saveLock = Lock() 

        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    #Helper func, parse domain of url and add to worker queue
    def add_worker_url(self, url):

        parsed = urlsplit(url)

        if not parsed.hostname:
            return

        #Retrieve proper domain
        domain = parsed.hostname.lower()
        if (domain.endswith("ics.uci.edu")):
            domain = "ics.uci.edu"
            self.tbd_worker[0].append(url)
        elif (domain.endswith("cs.uci.edu")):
            domain = "cs.uci.edu"
            self.tbd_worker[1].append(url)
        elif (domain.endswith("informatics.uci.edu")):
            domain = "ics.uci.edu"
            self.tbd_worker[2].append(url)
        elif (domain.endswith("stat.uci.edu")):
            domain = "ics.uci.edu"
            self.tbd_worker[3].append(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)
                self.add_worker_url(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self, wid):
        with self.tbdLock:
            # try own queue first
            if self.tbd_worker[wid]:
                return self.tbd_worker[wid].pop()

            # try others
            for i in range(4):
                if i == wid:
                    continue
                if self.tbd_worker[i]:
                    return self.tbd_worker[i].pop()

            return None

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        with self.saveLock:
            if urlhash not in self.save:
                self.save[urlhash] = (url, False)
                self.save.sync()
                with self.tbdLock:
                    self.to_be_downloaded.append(url)
                    self.add_worker_url(url)
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        with self.saveLock:
            if urlhash not in self.save:
                # This should not happen.
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")
            self.save[urlhash] = (url, True)
        self.save.sync()
