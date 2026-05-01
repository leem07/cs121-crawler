from configparser import ConfigParser
from argparse import ArgumentParser

from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler


def main(config_file, restart):
    print("[1] Loading config file...")

    cparser = ConfigParser()
    cparser.read(config_file)

    print("[2] Parsing config...")
    config = Config(cparser)

    print("[3] Starting cache server...")
    config.cache_server = get_cache_server(config, restart)

    print("[4] Initializing crawler...")
    crawler = Crawler(config, restart)

    print("[5] Starting crawler...")
    crawler.start()

    print("[6] Crawler finished.")
    
    num_unique_pages, longest_page, top_50, subdomain_list = crawler.return_report_stats()
    print(f"num_unique_pages = {num_unique_pages}")
    print(f"longest_page = {longest_page}")
    print(f"top_50 = {top_50}")
    print(f"subdomain_list = {subdomain_list}")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args()
    main(args.config_file, args.restart)
