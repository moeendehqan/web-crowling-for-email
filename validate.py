import csv
import re
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import os

# ---------------- پیکربندی ---------------- #
INPUT_FILE = "emails.csv"
OUTPUT_FILE = "valid_emails.csv"
PROCESSED_FILE = "processed_emails.csv"
SAVE_INTERVAL = 100  # گزارش و ذخیره موقت هر 100 ایمیل
MAX_WORKERS = 20

# دامنه‌های مشهور و شناخته‌شده که MX بررسی نمی‌شوند
WHITELIST_DOMAINS = {"gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com"}

lock = Lock()
valid_emails = set()
processed_emails = set()

EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

# ---------------- Resolver با timeout کوتاه ---------------- #
resolver = dns.resolver.Resolver()
resolver.lifetime = 3
resolver.timeout = 3

# ---------------- توابع اعتبارسنجی ---------------- #
def is_valid_format(email: str) -> bool:
    return bool(EMAIL_REGEX.match(email.strip()))

def is_domain_valid(email: str) -> bool:
    domain = email.split("@")[-1].lower()
    if domain in WHITELIST_DOMAINS:
        return True
    try:
        records = resolver.resolve(domain, 'MX')
        return len(records) > 0
    except:
        return False

def save_state():
    with lock:
        # ذخیره ایمیل‌های معتبر
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["email"])
            for email in valid_emails:
                writer.writerow([email])
        # ذخیره ایمیل‌های پردازش شده
        with open(PROCESSED_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["email"])
            for email in processed_emails:
                writer.writerow([email])

def validate_email(email: str):
    email = email.strip()
    if not email or email in processed_emails:
        return

    if is_valid_format(email) and is_domain_valid(email):
        with lock:
            valid_emails.add(email)

    with lock:
        processed_emails.add(email)
        if len(processed_emails) % SAVE_INTERVAL == 0:
            print(f"💾 ذخیره موقت: {len(processed_emails)} ایمیل بررسی شده، {len(valid_emails)} معتبر")
            save_state()

# ---------------- خواندن ایمیل‌ها ---------------- #
emails = []
if os.path.exists(INPUT_FILE):
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if row:
                emails.append(row[0].strip())

# بارگذاری ایمیل‌های پردازش‌شده قبلی
if os.path.exists(PROCESSED_FILE):
    with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if row:
                processed_emails.add(row[0].strip())

# بارگذاری ایمیل‌های معتبر قبلی
if os.path.exists(OUTPUT_FILE):
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if row:
                valid_emails.add(row[0].strip())

# ---------------- اعتبارسنجی multi-thread ---------------- #
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(validate_email, email) for email in emails]
    for future in as_completed(futures):
        try:
            future.result()
        except Exception as e:
            print(f"❌ خطا در بررسی ایمیل: {e}")

# ---------------- ذخیره نهایی ---------------- #
save_state()
print(f"\n✅ اعتبارسنجی کامل شد: {len(valid_emails)} ایمیل معتبر از {len(processed_emails)} بررسی شده")
