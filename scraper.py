import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
from collections import defaultdict, Counter

unique_pages = set()
word_frequencies = Counter()
longest_page = {"url": "", "word_count": 0}
subdomains = defaultdict(set)
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


def scraper(url, resp):
    links = extract_next_links(url, resp)
    report_stats = [unique_pages, longest_page, word_frequencies, subdomains]
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

    # Only process valid responses with content
    if resp.status != 200 or not resp.raw_response or not resp.raw_response.content:
        return extracted_links

    # Avoid very large files (over 10MB)
    content = resp.raw_response.content
    if len(content) > 10 * 1024 * 1024:
        return extracted_links

    try:
        soup = BeautifulSoup(content, "lxml")

        text = soup.get_text()
        words = re.findall(r"[a-zA-Z]{2,}", text.lower())
        filtered_words = [w for w in words if w not in STOP_WORDS]

        # skip low information pages (fewer than 50 words)
        if len(filtered_words) < 50:
            return extracted_links

        # Track word frequencies
        word_counter = Counter(filtered_words)
        word_frequencies.update(word_counter)

        # defrag the url
        defragged_url, _ = urldefrag(url)

        # Track unique pages
        unique_pages.add(defragged_url)

        # Track long page
        if len(words) > longest_page["word_count"]:
            longest_page["url"] = defragged_url
            longest_page["word_count"] = len(words)

        # Track subdomains
        parsed = urlparse(defragged_url)
        netloc = parsed.netloc
        if netloc.endswith(".ics.uci.edu"):
            subdomains[netloc].add(defragged_url)

        # link extraction
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"].strip()

            # Resolve relative URLs
            full_url = urljoin(url, href)

            # Remove fragment
            full_url, _ = urldefrag(full_url)

            if full_url:
                extracted_links.append(full_url)

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
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$",
            parsed.path.lower()
        ):
            return False

        netloc = parsed.netloc.lower()

        if not (
            netloc == "ics.uci.edu" or netloc.endswith(".ics.uci.edu") or
            netloc == "cs.uci.edu" or netloc.endswith(".cs.uci.edu") or
            netloc == "informatics.uci.edu" or netloc.endswith(".informatics.uci.edu") or
            netloc == "stat.uci.edu" or netloc.endswith(".stat.uci.edu")
        ):
            return False

        # avoid repeated trap segments
        path_segments = [s for s in parsed.path.split("/") if s]
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
        if re.search(r"(calendar|date|event)", parsed.path.lower()):
            if re.search(r"\d{4}-\d{2}-\d{2}|\d{8}", parsed.query + parsed.path):
                return False

        return True

    except TypeError:
        print ("TypeError for ", url)
        return False

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
        print ("TypeError for ", url)
        return False

def write_url_stats(url):
    pass
