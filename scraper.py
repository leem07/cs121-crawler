import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
from collections import defaultdict, Counter
from threading import Lock

'''
unique_pages = set()
word_frequencies = Counter()
longest_page = {"url": "", "word_count": 0}
subdomains = defaultdict(set)
'''

STOP_WORDS = set([
    "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your",
    "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she",
    "her", "hers", "herself", "it", "its", "itself", "they", "them", "their",
    "theirs", "themselves", "what", "which", "who", "whom", "this", "that",
    "these", "those", "am", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an",
    "the", "and", "but", "if", "or", "because", "as", "until", "while", "of",
    "at", "by", "for", "with", "about", "against", "between", "into", "through",
    "during", "before", "after", "above", "below", "to", "from", "up", "down",
    "in", "out", "on", "off", "over", "under", "again", "further", "then",
    "once", "here", "there", "when", "where", "why", "how", "all", "both",
    "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not",
    "only", "own", "same", "so", "than", "too", "very", "s", "t", "just",
    "don", "should", "now", "d", "ll", "m", "o", "re", "ve", "y", "ain",
    "aren", "couldn", "didn", "doesn", "hadn", "hasn", "haven", "isn", "ma",
    "mightn", "mustn", "needn", "shan", "shouldn", "wasn", "weren", "won", "wouldn"
])

url_stats = {}          # {subdomain: [num_unique_pages, {path1: num1, path2, num2}]}

_SIMHASH_BITS = 64
_NEAR_DUPLICATE_THRESH = 3

_simhash_store: list = []
_simhash_lock = Lock()


url_stats = {}          # {subdomain: [num_unique_pages, {path1: num1, path2, num2}]}

_SIMHASH_BITS = 64
_NEAR_DUPLICATE_THRESH = 3

_simhash_store: list = []
_simhash_lock = Lock()


# Convert the string into a unique 64 bit integer, need it for simhash.

def _fnv1a_64(token: str) -> int:
    h = 14695981039346656037         # FNV offset basis (64-bit)
    for byte in token.encode("utf-8"):
        h ^= byte
        h = (h * 1099511628211) & 0xFFFFFFFFFFFFFFFF
    return h

# 64 bit simhash fingerprint from a counter

def _simhash(word_counts: Counter) -> int:
    v = [0] * _SIMHASH_BITS
    for word, freq in word_counts.items():  # weight - term frequency
        h = _fnv1a_64(word)
        for i in range(_SIMHASH_BITS):
            if (h >> i) & 1:
                v[i] += freq
            else:
                v[i] -= freq
    fp = 0
    for i in range(_SIMHASH_BITS):
        if v[i] > 0:
            fp |= (1 << i)
    return fp


def _hamming(a: int, b: int) -> int:
    x = a ^ b
    count = 0
    while x:
        x &= x - 1  # clear lowest set bit
        count += 1
    return count


def _is_near_duplicate(word_counts: Counter, url: str) -> bool:
    fp = _simhash(word_counts)
    with _simhash_lock:
        for stored_fp, _ in _simhash_store:
            if _hamming(fp, stored_fp) <= _NEAR_DUPLICATE_THRESH:
                return True
        _simhash_store.append((fp, url))
    return False


def scraper(url, resp):
    # FIX: unpack both links AND report_stats from extract_next_links
    links, report_stats = extract_next_links(url, resp)
    # FIX: check resp.status, not resp
    if resp.status == 200:
        print("Scraping", url)
    elif 600 <= resp.status <= 699:
        print(f"Cache server error {resp.status} for {url}: {resp.error}")
    return [link for link in links if is_valid(link)], report_stats



    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content

def extract_next_links(url, resp):
    extracted_links = []
    empty_stats = (set(), {"url": "", "word_count": 0}, Counter(), defaultdict(set))
    # Only process valid responses with content
    if resp.status != 200 or not resp.raw_response or not resp.raw_response.content:
        return extracted_links, empty_stats

    # Avoid very large files (over 10MB)
    content = resp.raw_response.content
    if len(content) > 10 * 1024 * 1024:
        return extracted_links, empty_stats

    try:
        soup = BeautifulSoup(content, "lxml")

        text = soup.get_text()
        words = re.findall(r"[a-zA-Z]{2,}", text.lower())
        filtered_words = [w for w in words if w not in STOP_WORDS]

        # skip low information pages (fewer than 50 words)
        if len(filtered_words) < 50:
            return extracted_links, empty_stats

        word_counter = Counter(filtered_words)

        # Near-duplicate check using weighted SimHash
        if _is_near_duplicate(word_counter, url):
            print(f"Near-duplicate, skipping: {url}")
            return extracted_links, empty_stats

        # defrag the url
        defragged_url, _ = urldefrag(url)

        call_unique_pages = {defragged_url}

        # word count excludes HTML markup — use words (unfiltered) per spec
        call_longest_page = {"url": defragged_url, "word_count": len(words)}

        # Subdomain tracking
        call_subdomains = defaultdict(set)
        parsed = urlparse(defragged_url)
        netloc = parsed.netloc.lower()
        # Get rid of www.
        if netloc.startswith("www."):
            netloc = netloc[4:]

        if (netloc.endswith(".ics.uci.edu") or
                netloc.endswith(".cs.uci.edu") or
                netloc.endswith(".informatics.uci.edu") or
                netloc.endswith(".stat.uci.edu") or
                netloc in {"ics.uci.edu", "cs.uci.edu",
                           "informatics.uci.edu", "stat.uci.edu"}):
            call_subdomains[netloc].add(defragged_url)


        for anchor in soup.find_all("a", href=True):
            href = anchor["href"].strip()
            full_url = urljoin(url, href)
            full_url, _ = urldefrag(full_url)
            if full_url:
                extracted_links.append(full_url)

        report_stats = (call_unique_pages, call_longest_page,
                        word_counter, call_subdomains)
        return extracted_links, report_stats

    except Exception as e:
        print(f"Error processing {url}: {e}")

    return extracted_links


def is_valid(url):
    # Decide whether to crawl this url or not.
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)

        if parsed.scheme not in {"http", "https"}:
            return False

        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv|ppsx|img|apk|sql|pps"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$",
            parsed.path.lower()
        ):
            return False

        netloc = parsed.netloc.lower()
        path = parsed.path.lower()

        if not netloc.endswith(("ics.uci.edu",
                        "cs.uci.edu",
                        "informatics.uci.edu",
                        "stat.uci.edu")):
            return False

        # edge cases
        if url == "http://www.ics.uci.edu/~shantas/publications/20-secret-sharing-aggregation-TKDE-shantanu":
            return False
        if url == "http://www.ics.uci.edu/goodrich":
            return False
        if url == "http://www.ics.uci.edu/group":
            return False
        if url == "https://ics.uci.edu/~dechter/talks/DeepLearn17-Outline":
            return False

        # grape wiki traps
        if netloc == "grape.ics.uci.edu" and path.startswith("/wiki"):
            return False

        # isg event trap
        if netloc == "isg.ics.uci.edu" and re.search(r"/event/", path):
            return False

        # doku.php
        if "doku.php" in path:
            return False

        # wics  -- block if query params are present and calendar events
        if netloc == "wics.ics.uci.edu" and parsed.query:
            return False
        
        # eppstein infinite pictures
        if "~eppstein/pix" in path:
            return False

        # chemdb
        if netloc == "chemdb.ics.uci.edu" or netloc == "cdb.ics.uci.edu":
            return False

        # ngs.ics.uci.edu
        # if netloc == "ngs.ics.uci.edu" and "author/ramesh/page" in path:
        #     return False

        # ~baldig/learning empty pages
        if netloc == "ics.uci.edu" and "~baldig/learning" in path:
            return False;

        # dechter htmls
        if "~dechter" in path and "/r" in path:
            return False


        # avoid repeated trap segments
        path_segments = [s for s in path.split("/") if s]
        segment_counts = defaultdict(int)

        for seg in path_segments:
            segment_counts[seg] += 1
            if segment_counts[seg] > 2:
                return False

        # avoid very long URLs
        if len(url) > 500:
            return False

        # avoid too many parameters
        if parsed.query.count("&") > 5:
            return False

        # avoid calendar/date traps
        if re.search(r"(calendar|date|event|events)", path.lower()):
            if re.search(r"\d{4}-\d{2}(?:-\d{2})?|\d{8}", parsed.query + path):
                return False

        return True

    except TypeError:
        print ("TypeError for ", url)
        return False
