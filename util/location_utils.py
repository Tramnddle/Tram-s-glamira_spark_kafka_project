import pyspark.sql.functions as f
from pyspark.sql.types import StringType


COUNTRY_BY_TLD = {
    "ar": "Argentina",
    "at": "Austria",
    "au": "Australia",
    "be": "Belgium",
    "bg": "Bulgaria",
    "br": "Brazil",
    "ca": "Canada",
    "ch": "Switzerland",
    "cl": "Chile",
    "co": "Colombia",
    "cz": "Czech Republic",
    "de": "Germany",
    "dk": "Denmark",
    "ee": "Estonia",
    "es": "Spain",
    "fi": "Finland",
    "fr": "France",
    "gr": "Greece",
    "hr": "Croatia",
    "hu": "Hungary",
    "ie": "Ireland",
    "il": "Israel",
    "in": "India",
    "it": "Italy",
    "jp": "Japan",
    "lt": "Lithuania",
    "lu": "Luxembourg",
    "lv": "Latvia",
    "mx": "Mexico",
    "nl": "Netherlands",
    "no": "Norway",
    "nz": "New Zealand",
    "pl": "Poland",
    "pt": "Portugal",
    "ro": "Romania",
    "se": "Sweden",
    "sg": "Singapore",
    "si": "Slovenia",
    "sk": "Slovakia",
    "tr": "Turkey",
    "uk": "United Kingdom",
    "us": "United States",
}


def extract_country_code_from_domain(domain):
    if domain is None:
        return None

    parts = [part for part in domain.strip().lower().split(".") if part]
    if not parts:
        return None

    tld = parts[-1]
    if len(tld) == 2 and tld.isalpha():
        return tld.upper()

    return None


def extract_country_from_domain(domain):
    country_code = extract_country_code_from_domain(domain)
    if country_code is None:
        return None

    return COUNTRY_BY_TLD.get(country_code.lower(), country_code)


extract_country_code_udf = f.udf(extract_country_code_from_domain, StringType())
extract_country_udf = f.udf(extract_country_from_domain, StringType())
