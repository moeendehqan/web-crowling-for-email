import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, urlunparse


def normalize_url(url: str) -> str:
    """لینک رو نرمالایز می‌کنه (https و حذف // اضافی)."""
    parsed = urlparse(url)
    scheme = "https" if parsed.scheme in ["http", "https"] else parsed.scheme
    path = parsed.path.replace("//", "/")
    return urlunparse((scheme, parsed.netloc.lower(), path, parsed.params, parsed.query, parsed.fragment))


def get_base_url(url: str) -> str:
    """فقط دامین رو برمی‌گردونه (بدون www)."""
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def allow_url(url: str) -> bool:
    """بررسی کنه لینک جزو بلاک لیست نباشه."""
    disallow = {
        'google.com', 'facebook.com', 'instagram.com', 'twitter.com',
        'youtube.com', 'linkedin.com', 'pinterest.com', 'reddit.com',
        'tumblr.com', 'yahoo.com', 'aparat.com', 'x.com', 't.me'
    }
    return get_base_url(url) not in disallow


def full_url(link: str, base_url: str) -> str:
    """اضافه کردن base_url به لینک‌های relative و نرمالایز کردن نهایی."""
    if not base_url.endswith("/"):
        base_url += "/"
    absolute_url = urljoin(base_url, link)
    return normalize_url(absolute_url)
