import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, urlunparse, parse_qs
import urllib3
import re
import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

lock = Lock()
SAVE_INTERVAL = 40
MAX_WORKERS = 40
MAX_LINKS = 100_000   # 🔹 سقف لینک‌ها

session = requests.Session()

# ---------------- Utils ---------------- #
def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    scheme = "https" if parsed.scheme in ["http", "https"] else "http"
    path = parsed.path.replace("//", "/")
    return urlunparse((scheme, parsed.netloc.lower(), path, parsed.params, parsed.query, parsed.fragment))

def get_base_url(url: str) -> str:
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    return domain[4:] if domain.startswith("www.") else domain

def full_url(link: str, base_url: str) -> str:
    url = normalize_url(urljoin(base_url, link))
    parsed = urlparse(url)
    if "google." in parsed.netloc and parsed.path == "/url":
        qs = parse_qs(parsed.query)
        if "q" in qs:
            return normalize_url(qs["q"][0])
    return url

def extract_emails(text: str) -> set:
    return set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text))

def is_media_or_document(url: str) -> bool:
    return urlparse(url).path.lower().endswith((
        ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".svg",
        ".mp4", ".mov", ".avi", ".mkv", ".webm"
    ))

# ---------------- Load existing data ---------------- #
links = []
seen_links = set()
if os.path.exists("links.csv"):
    with open("links.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            links.append({"link": row["link"], "a_crawl": int(row["a_crawl"])})
            seen_links.add(row["link"])

emails = set()
if os.path.exists("emails.csv"):
    with open("emails.csv", "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            emails.add(row[0].strip())

pages_processed = 0

# ---------------- Save functions ---------------- #
def save_links():
    global links
    if len(links) > MAX_LINKS:  # 🔹 مدیریت حجم لینک‌ها
        links = [l for l in links if l["a_crawl"] == 0]
        print(f"⚠️ حجم لینک‌ها زیاد بود → فقط {len(links)} لینک باقیمانده ذخیره شد")

    with open("links.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["link", "a_crawl"])
        writer.writeheader()
        for l in links:
            writer.writerow(l)

def save_emails(new_emails):
    with open("emails.csv", "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for email in new_emails:
            writer.writerow([email])

def mark_as_crawled(page_url: str):
    with lock:
        for l in links:
            if l["link"] == page_url:
                l["a_crawl"] = 1
                break

# ---------------- Crawl Function ---------------- #
def crawl_page(page_url: str):
    global pages_processed

    if is_media_or_document(page_url):
        mark_as_crawled(page_url)
        return

    try:
        response = session.get(page_url, verify=False, timeout=6)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        local_emails = extract_emails(soup.get_text(" ", strip=True))

        local_links = set()
        for a in soup.find_all('a', href=True):
            full = full_url(a['href'], page_url)
            if is_media_or_document(full):
                continue
            if "google." in urlparse(page_url).netloc:
                if "google." not in urlparse(full).netloc:
                    local_links.add(full)
            else:
                local_links.add(full)

        with lock:
            new_emails = local_emails - emails
            if new_emails:
                emails.update(new_emails)
                save_emails(new_emails)
                print(f"📧 {len(new_emails)} ایمیل جدید")

            for link in local_links:
                if link not in seen_links:
                    links.append({"link": link, "a_crawl": 0})
                    seen_links.add(link)

            pages_processed += 1
            if pages_processed % SAVE_INTERVAL == 0:
                save_links()
                print(f"💾 ذخیره دوره‌ای ({pages_processed} صفحه).")

        print(f"✅ {page_url} | {len(local_emails)} ایمیل، {len(local_links)} لینک")

    except Exception as e:
        print(f"❌ خطا در {page_url}: {e}")

    finally:
        mark_as_crawled(page_url)

# ---------------- Run in Loop ---------------- #
while True:
    to_crawl = [l["link"] for l in links if l["a_crawl"] == 0]
    if not to_crawl:
        break

    print(f"\n🚀 شروع دور جدید: {len(to_crawl)} لینک در صف")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(crawl_page, url): url for url in to_crawl}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"❌ خطای ناخواسته در {futures[future]}: {e}")

    save_links()

print(f"\n🏁 کار تمام شد! {sum(1 for l in links if l['a_crawl']==0)} لینک باقی و {len(emails)} ایمیل ذخیره شد.")
