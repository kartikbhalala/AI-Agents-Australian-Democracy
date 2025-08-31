#!/usr/bin/env python3
"""
Respectful crawler for peo.gov.au that saves cleaned page text to .txt files
and a manifest.jsonl for easy downstream indexing.

- Stays within peo.gov.au
- Respects robots.txt and uses sitemap when available
- Skips binaries (PDFs, images, etc.)
- Extracts title, meta description, and main/article/body text
- Default hard cap: 20 pages (demo-friendly)
"""

import argparse
import hashlib
import json
import os
import re
import time
from collections import deque
from datetime import datetime
from urllib.parse import urljoin, urlparse, urldefrag
from urllib.robotparser import RobotFileParser
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def make_session(user_agent: str = "CommunityMateCrawler/0.1 (+https://example.com/demo)"):
    """Requests session with retries and a friendly UA."""
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        raise_on_status=False,
    )
    s.headers.update({
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.9",
    })
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


def normalize_url(u: str) -> str:
    """Remove fragments, normalize path (no trailing slash except root)."""
    if not u:
        return u
    u, _ = urldefrag(u)
    parsed = urlparse(u)
    path = parsed.path or "/"
    if path.endswith("/") and path != "/":
        path = path[:-1]
    normalized = parsed._replace(path=path).geturl()
    return normalized


def same_domain(u: str, root: str) -> bool:
    """Stay strictly on the same host."""
    try:
        return urlparse(u).netloc.lower() == urlparse(root).netloc.lower()
    except Exception:
        return False


EXCLUDED_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico",
    ".css", ".js", ".map",
    ".pdf", ".zip", ".gz", ".rar", ".7z",
    ".mp4", ".webm", ".mp3", ".wav", ".avi",
    ".ttf", ".woff", ".woff2", ".eot",
    ".xml"  # skip general XML pages in crawl (except explicit sitemap fetching)
}


def looks_like_binary(u: str) -> bool:
    path = urlparse(u).path.lower()
    return any(path.endswith(ext) for ext in EXCLUDED_EXTENSIONS)


def is_html_response(resp: requests.Response) -> bool:
    ctype = (resp.headers.get("Content-Type") or "").lower()
    return "text/html" in ctype or "application/xhtml+xml" in ctype


def load_robots(start_url: str) -> RobotFileParser:
    parsed = urlparse(start_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = RobotFileParser()
    rp.set_url(robots_url)
    try:
        rp.read()
    except Exception:
        pass
    return rp


def discover_sitemaps(rp: RobotFileParser, start_url: str):
    """Return list of sitemap URLs (if any), otherwise a common default."""
    sitemaps = []
    try:
        sm = rp.site_maps()
        if sm:
            sitemaps.extend(sm)
    except Exception:
        pass
    if not sitemaps:
        parsed = urlparse(start_url)
        sitemaps.append(f"{parsed.scheme}://{parsed.netloc}/sitemap.xml")
    return sitemaps


def parse_sitemap(session: requests.Session, url: str, timeout=12) -> list:
    """
    Parse sitemap or sitemap index; return list of URLs.
    Handles <urlset> and <sitemapindex>.
    """
    urls = []
    try:
        r = session.get(url, timeout=timeout)
        if r.status_code != 200 or "xml" not in (r.headers.get("Content-Type") or "").lower():
            return urls
        root = ET.fromstring(r.content)
        tag = root.tag.lower()
        if tag.endswith("sitemapindex"):
            for sm in root.findall(".//{*}sitemap/{*}loc"):
                loc = (sm.text or "").strip()
                if loc:
                    urls.extend(parse_sitemap(session, loc))
        elif tag.endswith("urlset"):
            for u in root.findall(".//{*}url/{*}loc"):
                loc = (u.text or "").strip()
                if loc:
                    urls.append(loc)
    except Exception:
        return urls
    return urls


def extract_main_text(html: str) -> tuple[str, str, str]:
    """Extract (title, meta_description, main_text) from HTML."""
    soup = BeautifulSoup(html, "html.parser")

    # Remove noisy blocks
    for tag_name in ["script", "style", "noscript", "form", "aside"]:
        for t in soup.find_all(tag_name):
            t.decompose()
    for selector in ["nav", "footer"]:
        for t in soup.select(selector):
            t.decompose()

    title = soup.title.get_text(strip=True) if soup.title else ""

    meta_desc = ""
    md = soup.find("meta", attrs={"name": "description"})
    if not md:
        md = soup.find("meta", attrs={"property": "og:description"})
    if md and md.get("content"):
        meta_desc = md["content"].strip()

    # Prefer <main>, then <article>, then body
    node = soup.find("main") or soup.find("article") or soup.body or soup
    text = node.get_text(separator="\n", strip=True) if node else soup.get_text(separator="\n", strip=True)

    # Clean whitespace
    text = re.sub(r"\r+", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)

    return title, meta_desc, text


def slug_for_file(url: str, maxlen: int = 180) -> str:
    """
    Safe filename from URL path (plus short hash).
    Example: /learn/how-parliament-works -> learn__how-parliament-works__<hash8>.txt
    """
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    if not path:
        path = "home"
    qhash = ""
    if parsed.query:
        qhash = hashlib.sha1(parsed.query.encode("utf-8")).hexdigest()[:8]
    raw = path.replace("/", "__")
    raw = re.sub(r"[^a-zA-Z0-9\-_\.]+", "-", raw)
    if len(raw) > maxlen:
        raw = raw[:maxlen]
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()[:8]
    base = f"{raw}__{h}"
    if qhash:
        base += f"_{qhash}"
    return base.lower()


def save_txt(out_dir: str, url: str, title: str, meta_desc: str, text: str):
    os.makedirs(out_dir, exist_ok=True)
    fname = slug_for_file(url) + ".txt"
    path = os.path.join(out_dir, fname)

    header = [
        f"URL: {url}",
        f"TITLE: {title}",
        f"DESC: {meta_desc}",
        f"CRAWLED_AT: {datetime.utcnow().isoformat()}Z",
        f"WORD_COUNT: {len(text.split())}"
    ]
    content = "\n".join(header) + "\n---\n" + text + "\n"
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path


def append_manifest(manifest_path: str, record: dict):
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    with open(manifest_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def extract_links(html: str, base_url: str) -> list:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if not href:
            continue
        if href.startswith(("mailto:", "tel:", "javascript:", "#")):
            continue
        abs_url = urljoin(base_url, href)
        links.append(abs_url)
    return links


def crawl_peo(
    start_url: str,
    out_dir: str = "data/peo",
    delay: float = 1.5,
    max_pages: int = 20,   # demo default
    use_sitemap: bool = True,
):
    session = make_session()
    rp = load_robots(start_url)
    agent = session.headers.get("User-Agent", "*")

    base_host = urlparse(start_url).netloc

    pages_dir = os.path.join(out_dir, "pages")
    manifest_path = os.path.join(out_dir, "manifest.jsonl")
    os.makedirs(pages_dir, exist_ok=True)

    # Seed queue
    queue = deque()
    seen = set()

    if use_sitemap:
        sitemaps = discover_sitemaps(rp, start_url)
        seeded = False
        for sm in sitemaps:
            urls = parse_sitemap(session, sm)
            urls = [u for u in urls if same_domain(u, start_url)]
            if urls:
                for u in urls:
                    n = normalize_url(u)
                    if not looks_like_binary(n):
                        queue.append(n)
                seeded = True
        if not seeded:
            queue.append(normalize_url(start_url))
    else:
        queue.append(normalize_url(start_url))

    print(f"Seeded {len(queue)} URLs. Starting crawl (max_pages={max_pages}, delay={delay}s)")

    count = 0
    while queue and count < max_pages:
        url = queue.popleft()
        if url in seen:
            # Still sleep a bit to be gentle even if we skip
            time.sleep(delay)
            continue
        seen.add(url)

        if not same_domain(url, start_url):
            time.sleep(delay)
            continue

        # robots.txt check
        try:
            if not rp.can_fetch(agent, url):
                print(f"[robots] Skipping: {url}")
                time.sleep(delay)
                continue
        except Exception:
            pass

        if looks_like_binary(url):
            time.sleep(delay)
            continue

        try:
            resp = session.get(url, timeout=12)
        except Exception as e:
            print(f"[error] {url} -> {e}")
            time.sleep(delay)
            continue

        if resp.status_code != 200:
            print(f"[{resp.status_code}] {url}")
            time.sleep(delay)
            continue

        if not is_html_response(resp):
            # Skip non-HTML (e.g., PDFs). Add PDF parsing later if needed.
            time.sleep(delay)
            continue

        html = resp.text
        title, meta_desc, main_text = extract_main_text(html)
        path = save_txt(pages_dir, url, title, meta_desc, main_text)
        count += 1

        append_manifest(
            manifest_path,
            {
                "url": url,
                "title": title,
                "desc": meta_desc,
                "path": path,
                "crawled_at": datetime.utcnow().isoformat() + "Z",
                "host": base_host,
                "word_count": len(main_text.split()),
            },
        )
        print(f"[{count}] Saved: {url} -> {path}")

        # Enqueue new links
        for link in extract_links(html, url):
            n = normalize_url(link)
            if n not in seen and same_domain(n, start_url) and not looks_like_binary(n):
                queue.append(n)

        # Be polite
        time.sleep(delay)

    print(f"Done. Saved {count} pages to {pages_dir}")
    print(f"Manifest: {manifest_path}")


def main():
    parser = argparse.ArgumentParser(description="Respectful PEO crawler -> .txt files")
    parser.add_argument("--start", type=str, default="https://peo.gov.au/", help="Start URL (root of crawl)")
    parser.add_argument("--out", type=str, default="data/peo", help="Output directory")
    parser.add_argument("--delay", type=float, default=1.5, help="Delay between requests (seconds)")
    parser.add_argument("--max-pages", type=int, default=20, help="Maximum number of pages to crawl (demo default = 20)")
    parser.add_argument("--no-sitemap", action="store_true", help="Do not seed from sitemap")
    args = parser.parse_args()

    # Hard cap for demo: never exceed 20
    args.max_pages = min(args.max_pages, 20)

    crawl_peo(
        start_url=args.start,
        out_dir=args.out,
        delay=args.delay,
        max_pages=args.max_pages,
        use_sitemap=not args.no_sitemap,
    )


if __name__ == "__main__":
    main()