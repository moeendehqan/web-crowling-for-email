import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, urlunparse
import urllib3
import re
import csv
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ---------------- Utils ---------------- #
def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    scheme = "https" if parsed.scheme in ["http", "https"] else parsed.scheme
    path = parsed.path.replace("//", "/")
    return urlunparse((scheme, parsed.netloc.lower(), path, parsed.params, parsed.query, parsed.fragment))


def get_base_url(url: str) -> str:
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def allow_url(url: str) -> bool:
    disallow = {
        'google.com', 'facebook.com', 'instagram.com', 'twitter.com',
        'youtube.com', 'linkedin.com', 'pinterest.com', 'reddit.com',
        'tumblr.com', 'yahoo.com', 'aparat.com', 'x.com', 't.me'
    }
    return get_base_url(url) not in disallow


def full_url(link: str, base_url: str) -> str:
    if not base_url.endswith("/"):
        base_url += "/"
    absolute_url = urljoin(base_url, link)
    return normalize_url(absolute_url)


def extract_emails(text: str) -> set:
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    return set(re.findall(email_pattern, text))


def is_media_or_document(url: str) -> bool:
    """بررسی می‌کند که لینک به PDF، عکس یا ویدیو اشاره نکند."""
    media_extensions = (
        ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".svg",
        ".mp4", ".mov", ".avi", ".mkv", ".webm"
    )
    parsed = urlparse(url)
    path = parsed.path.lower()
    return path.endswith(media_extensions)


# ---------------- Load existing data ---------------- #
df_link = pd.read_excel('link.xlsx')
df_link = df_link.dropna(subset=['link'])

emails = set()
if os.path.exists("emails.csv"):
    with open("emails.csv", "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)  # رد کردن هدر
        for row in reader:
            emails.add(row[0].strip())


# ---------------- Main Crawl ---------------- #
new_links = set()

for index, row in df_link.iterrows():
    try:
        page_url = row['link']
        a_crawl = row['a_crawl']

        if a_crawl == 1:
            continue

        print(f"\n🌐 در حال پردازش: {page_url}")
        if is_media_or_document(page_url):
            print(f"❌ {page_url} is media or document")
            continue

        try:
            response = requests.get(page_url, verify=False, timeout=10)
        except Exception as e:
            print(f"❌ خطا در دریافت {page_url}: {e}")
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        found_links = [a['href'] for a in soup.find_all('a', href=True)]

        # 🔎 استخراج ایمیل‌ها
        page_emails = extract_emails(response.text)
        for email in page_emails:
            try:
                if email not in emails:
                    emails.add(email)
                    print(f"📧 ایمیل جدید پیدا شد: {email}")
            except Exception as e:
                print(f"❌ خطا در اضافه کردن ایمیل {email}: {e}")
                continue

        # 🔗 پردازش لینک‌ها
        for raw_link in found_links:
            try:
                full = full_url(raw_link, page_url)
                if not allow_url(full):
                    continue
                if is_media_or_document(full):
                    continue  # رد کردن لینک‌های PDF، عکس و ویدیو
                if full not in df_link['link'].values and full not in new_links:
                    new_links.add(full)
                    print(f"➕ لینک جدید اضافه شد: {full}")
            except Exception as e:
                print(f"❌ خطا در اضافه کردن لینک {full}: {e}")
                continue
    except Exception as e:
        print(f"❌ خطا در پردازش {page_url}: {e}")
        continue

# بروزرسانی دیتافریم لینک‌ها
df_link.loc[:, 'a_crawl'] = 1
df_link = pd.concat(
    [df_link, pd.DataFrame({'link': list(new_links), 'a_crawl': 0})],
    ignore_index=True
)
df_link.to_excel('link.xlsx', index=False)

# ذخیره ایمیل‌ها
with open("emails.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["email"])
    for email in sorted(emails):
        writer.writerow([email])

print(f"\n✅ {len(new_links)} لینک جدید پیدا شد.")
print(f"📧 {len(emails)} ایمیل یونیک ذخیره شد (emails.csv).")
