import pyspark.sql.functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from urllib.parse import quote, unquote, urlparse, urlunparse


def normalize_string(column):
    return f.when(f.trim(column) == "", None).otherwise(f.trim(column))


def normalize_lower(column):
    return f.when(f.trim(column) == "", None).otherwise(f.lower(f.trim(column)))


def clean_url(raw_url):
    if raw_url is None:
        return None

    url = raw_url.strip()
    if url == "":
        return None

    try:
        parsed = urlparse(url)
        scheme = parsed.scheme.lower() if parsed.scheme else "http"
        netloc = parsed.netloc.lower()
        path = quote(unquote(parsed.path), safe="/:@&+$,;=")
        query = quote(unquote(parsed.query), safe="=&?/:@+,$;")
        cleaned = urlunparse((scheme, netloc, path, "", query, ""))
        if cleaned.endswith("/"):
            cleaned = cleaned[:-1]
        return cleaned
    except Exception:
        return None


clean_url_udf = udf(clean_url, StringType())


def extract_domain(url):
    if url is None:
        return None

    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        return domain if domain else None
    except Exception:
        return None


extract_domain_udf = udf(extract_domain, StringType())
