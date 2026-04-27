import re
from urllib.parse import urlparse
# from bs4 import BeautifulSoup

url_stats = {}          # {subdomain: [num_unique_pages, {path1: num1, path2, num2}]}


def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    return list()

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()):
            return False

        if parsed.netloc not in set(
            ["ics.uci.edu", "cs.uci.edu", "informatics.uci.edu","stat.uci.edu"]
            ) and not parsed.netloc.endswith("ics.uci.edu"):
            return False
        
        return True

    except TypeError:
        print ("TypeError for ", parsed)
        raise

# Return number of unique pages, number of subdomains
def update_url_stats(url):
    try:
        parsed = urlparse(url)
        subdomain = parsed.netloc
        path = parsed.path

        if subdomain in url_stats:
            if path not in url_stats[subdomain][1]:
                url_stats[subdomain][1][path] = 0
                url_stats[subdomain][0] += 1
        else:
            url_stats[subdomain] = [0, {path: 0}]

        return url_stats
    
    except TypeError:
        print ("TypeError for ", parsed)
        raise

def write_url_stats(url):
    pass
