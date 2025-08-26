import csv
import re
import dns.resolver
import smtplib
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import threading

# ---------------- پیکربندی ---------------- #
INPUT_FILE = "emails.csv"
OUTPUT_FILE = "valid_emails.csv"
PROCESSED_FILE = "processed_emails.csv"
SAVE_INTERVAL = 50
MAX_WORKERS = 10

# فعال/غیرفعال کردن SMTP Check
SMTP_CHECK_ENABLED = False  

# وزن‌دهی فاکتورها (قابل تغییر توسط شما)
WEIGHTS = {
    "regex": 2,
    "whitelist": 3,
    "mx": 2,
    "smtp": 4
}

QUALITY_THRESHOLD = 4  

# دامنه‌های خاص
WHITELIST_DOMAINS = {"gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com"}
BLACKLIST_DOMAINS = {"example.com", "test.com", "mailinator.com", "tempmail.com"}

# ---------------- Regex و Resolver ---------------- #
EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,24}$")

resolver = dns.resolver.Resolver()
resolver.lifetime = 3
resolver.timeout = 3

# Lock برای ایمن‌سازی Threadها
lock = threading.Lock()

# ---------------- توابع ---------------- #
def is_valid_format(email: str) -> bool:
    return bool(EMAIL_REGEX.match(email.strip()))

def has_mx_record(domain: str) -> bool:
    try:
        records = resolver.resolve(domain, 'MX')
        return len(records) > 0
    except:
        return False

def smtp_check(email: str) -> bool:
    if not SMTP_CHECK_ENABLED:
        return False
    try:
        domain = email.split("@")[-1]
        records = resolver.resolve(domain, 'MX')
        mx_record = str(records[0].exchange)
        server = smtplib.SMTP(timeout=5)
        server.connect(mx_record)
        server.helo(name="example.com")
        server.mail("test@example.com")
        code, _ = server.rcpt(email)
        server.quit()
        return code in [250, 251]
    except:
        return False

def validate_email(email: str, processed: set, valid: dict):
    email = email.strip().lower()
    if not email:
        return None

    with lock:
        if email in processed:
            return None

    domain = email.split("@")[-1]
    score = 0

    # blacklist
    if domain in BLACKLIST_DOMAINS:
        with lock:
            processed.add(email)
        return None

    # regex
    if is_valid_format(email):
        score += WEIGHTS["regex"]
    else:
        with lock:
            processed.add(email)
        return None

    # whitelist یا MX
    if domain in WHITELIST_DOMAINS:
        score += WEIGHTS["whitelist"]
    elif has_mx_record(domain):
        score += WEIGHTS["mx"]

    # smtp check
    if SMTP_CHECK_ENABLED and smtp_check(email):
        score += WEIGHTS["smtp"]

    with lock:
        processed.add(email)
        if score >= QUALITY_THRESHOLD:
            valid[email] = score

    return email, score

def save_state(valid: dict, processed: set):
    with lock:
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["email", "score"])
            for email, score in valid.items():
                writer.writerow([email, score])
        with open(PROCESSED_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["email"])
            for email in processed:
                writer.writerow([email])

# ---------------- اجرای اصلی ---------------- #
def main():
    emails = []
    valid_emails = {}
    processed_emails = set()

    # بارگذاری ورودی
    if os.path.exists(INPUT_FILE):
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if row:
                    emails.append(row[0].strip())

    # بارگذاری پردازش‌شده‌ها
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
                if row and len(row) > 1:
                    valid_emails[row[0]] = int(row[1])

    # multi-thread
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(validate_email, email, processed_emails, valid_emails) for email in emails]
        counter = 0
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    counter += 1
                    if counter % SAVE_INTERVAL == 0:
                        print(f"💾 ذخیره موقت: {len(processed_emails)} بررسی شده، {len(valid_emails)} معتبر")
                        save_state(valid_emails, processed_emails)
            except Exception as e:
                print(f"❌ خطا: {e}")

    save_state(valid_emails, processed_emails)
    print(f"\n✅ پایان: {len(valid_emails)} ایمیل معتبر از {len(processed_emails)} بررسی شد.")

if __name__ == "__main__":
    main()
