import csv
import re
from urllib.parse import urlparse

import requests
import pandas as pd

HEADERS = {
    "User-Agent": "grad-job-scanner (learning project; respectful)"
}

# --- Simple knobs you can tweak later ---
UK_HINTS = [
    "gb", "uk", "united kingdom",
    "london", "manchester", "birmingham", "leeds", "bristol",
    "glasgow", "edinburgh", "belfast", "cambridge", "oxford"
]

HIGH_TITLE_KEYWORDS = [
    "graduate", "junior", "trainee", "assistant", "coordinator",
    "administrator", "admin", "apprentice", "intern", "placement"
]

LESS_TITLE_KEYWORDS = [
    "analyst", "officer", "executive", "associate"
]

SENIOR_EXCLUDE_KEYWORDS = [
    # explicit seniority
    "senior", "sr", "lead", "manager", "principal", "head", "director", "vp",
    "vice president", "chief", "cmo", "cto", "cfo",

    # common “not-grad” titles that often slip through
    "product owner", "product manager", "programme manager", "program manager",
    "consultant", "specialist", "partner", "architect",

    # tech leveling
    "staff", "ii", "iii", "iv"
]


def contains_any(text: str, keywords: list[str]) -> bool:
    t = (text or "").lower()
    return any(k in t for k in keywords)


def is_uk(location: str) -> bool:
    loc = (location or "").strip().lower()
    return any(h in loc for h in UK_HINTS)


def extract_years(text: str):
    """
    Super-MVP year parsing. Returns (min_years, max_years) where either can be None.
    """
    t = (text or "").lower()

    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:-|–|to)\s*(\d+(?:\.\d+)?)\s*years?", t)
    if m:
        return float(m.group(1)), float(m.group(2))

    m = re.search(r"up to\s*(\d+(?:\.\d+)?)\s*years?", t)
    if m:
        return 0.0, float(m.group(1))

    m = re.search(r"(\d+(?:\.\d+)?)\s*\+\s*years?", t)
    if m:
        return float(m.group(1)), None

    return None, None


def bucket_job(title: str, location: str, description_text: str) -> tuple[str, str]:
    """
    Returns (bucket, reason)
    bucket = EXCLUDE | IGNORE | HIGH | LESS
    """
    title_l = (title or "").lower()

    # Exclude obvious senior titles
    if contains_any(title_l, SENIOR_EXCLUDE_KEYWORDS):
        return "EXCLUDE", "Senior keyword in title"

    # UK-only filter
    if not is_uk(location):
        return "IGNORE", "Not UK location"

    # Exclude obvious senior experience (5+)
    y_min, _y_max = extract_years(description_text or "")
    if y_min is not None and y_min >= 5:
        return "EXCLUDE", "5+ years mentioned"

    # Bucket by title signals
    if contains_any(title_l, HIGH_TITLE_KEYWORDS):
        return "HIGH", "Strong junior keyword in title"

    if contains_any(title_l, LESS_TITLE_KEYWORDS):
        return "LESS", "Stealth junior keyword in title"

    # Keep as LESS so you can review (MVP behaviour)
    return "LESS", "No strong signal (kept for review)"


# ---------------------------
# Fetch jobs from each platform
# ---------------------------

def fetch_json(url: str) -> dict:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def scrape_pinpoint(base_url: str) -> list[dict]:
    # base_url looks like: https://cfc.pinpointhq.com
    postings_url = base_url.rstrip("/") + "/postings.json"
    payload = fetch_json(postings_url)
    postings = payload.get("data") or []

    jobs = []
    for p in postings:
        jobs.append({
            "title": p.get("title", ""),
            "location": (p.get("location") or {}).get("name", "") or "",
            "department": (p.get("department") or {}).get("name", "") or "",
            "url": p.get("url", "") or "",
            "employment_type": p.get("employment_type_text", "") or p.get("employment_type", "") or "",
            "description": (p.get("description", "") or "")
        })
    return jobs


def scrape_greenhouse(boards_url: str) -> list[dict]:
    # boards_url looks like: https://boards.greenhouse.io/{company}
    # The JSON endpoint is: https://boards.greenhouse.io/{company}?gh_jid=... etc,
    # but easiest is: https://boards.greenhouse.io/{company}?format=json
    json_url = boards_url.rstrip("/") + "?format=json"
    payload = fetch_json(json_url)

    # Greenhouse varies: sometimes "jobs", sometimes "departments"
    jobs = []

    if "jobs" in payload:
        for j in payload.get("jobs", []):
            jobs.append({
                "title": j.get("title", ""),
                "location": (j.get("location") or {}).get("name", "") if isinstance(j.get("location"), dict) else (j.get("location") or ""),
                "department": "",
                "url": j.get("absolute_url", "") or "",
                "employment_type": "",
                "description": (j.get("content", "") or "")
            })

    # If it comes grouped by departments
    if "departments" in payload:
        for dep in payload.get("departments", []):
            dep_name = dep.get("name", "")
            for j in dep.get("jobs", []):
                jobs.append({
                    "title": j.get("title", ""),
                    "location": (j.get("location") or {}).get("name", "") if isinstance(j.get("location"), dict) else (j.get("location") or ""),
                    "department": dep_name,
                    "url": j.get("absolute_url", "") or "",
                    "employment_type": "",
                    "description": (j.get("content", "") or "")
                })

    return jobs


def scrape_workday(site_url: str) -> list[dict]:
    """
    MVP Workday approach:
    - You provide a Workday jobs site URL like:
        https://company.wd3.myworkdayjobs.com/CompanyCareers
      or:
        https://company.wd3.myworkdayjobs.com/en-US/CompanyCareers
    - We derive the "cxs" JSON search endpoint and paginate.

    NOTE: Workday sites vary; this works for many, not all. We'll tighten per employer later.
    """
    parsed = urlparse(site_url)
    host = parsed.netloc  # company.wdX.myworkdayjobs.com
    path = parsed.path.strip("/")

    # path might be "CompanyCareers" or "en-US/CompanyCareers"
    parts = path.split("/")
    if len(parts) == 1:
        site_slug = parts[0]
        locale = "en-US"
    else:
        locale = parts[0]
        site_slug = parts[1]

    # tenant usually is the subdomain before .wdX...
    tenant = host.split(".")[0]

    search_url = f"https://{host}/wday/cxs/{tenant}/{site_slug}/jobs"

    all_jobs = []
    offset = 0
    limit = 20

    while True:
        body = {
            "appliedFacets": {},
            "limit": limit,
            "offset": offset,
            "searchText": ""
        }

        r = requests.post(search_url, headers={**HEADERS, "Content-Type": "application/json"}, json=body, timeout=30)
        r.raise_for_status()
        payload = r.json()

        items = payload.get("jobPostings") or payload.get("items") or []
        if not items:
            break

        for it in items:
            title = it.get("title") or it.get("jobPostingTitle") or ""
            external_path = it.get("externalPath") or it.get("externalUrl") or ""
            # Build a reasonable URL if we only get a path
            if external_path and external_path.startswith("/"):
                url = f"https://{host}{external_path}"
            else:
                url = external_path

            # Locations vary widely; try a few keys
            location = (
                it.get("locationsText")
                or it.get("location")
                or it.get("primaryLocation")
                or ""
            )

            all_jobs.append({
                "title": title,
                "location": location,
                "department": "",
                "url": url,
                "employment_type": "",
                "description": ""  # MVP: we don't fetch full descriptions yet
            })

        offset += limit

        # Stop if we've reached total
        total = payload.get("total") or payload.get("totalCount")
        if total is not None and offset >= int(total):
            break

        # Safety stop (prevents infinite loops)
        if offset > 500:
            break

    return all_jobs


def load_employers(path: str = "employers.csv") -> list[dict]:
    employers = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            employers.append({
                "name": row.get("name", "").strip(),
                "type": row.get("type", "").strip().lower(),
                "url": row.get("url", "").strip(),
                "country": row.get("country", "").strip().upper() or "UK"
            })
    return employers


def main():
    employers = load_employers()

    rows = []

    for emp in employers:
        emp_name = emp["name"]
        emp_type = emp["type"]
        emp_url = emp["url"]

        try:
            if emp_type == "pinpoint":
                jobs = scrape_pinpoint(emp_url)
            elif emp_type == "greenhouse":
                jobs = scrape_greenhouse(emp_url)
            elif emp_type == "workday":
                jobs = scrape_workday(emp_url)
            else:
                print(f"Skipping {emp_name}: unsupported type '{emp_type}'")
                continue

            for j in jobs:
                bucket, reason = bucket_job(j["title"], j.get("location", ""), j.get("description", ""))

                if bucket in ("EXCLUDE", "IGNORE"):
                    continue

                rows.append({
                    "employer": emp_name,
                    "title": j["title"],
                    "location": j.get("location", ""),
                    "department": j.get("department", ""),
                    "employment_type": j.get("employment_type", ""),
                    "url": j.get("url", ""),
                    "bucket": bucket,
                    "reason": reason,
                })

            print(f"{emp_name}: kept {sum(1 for r in rows if r['employer']==emp_name)} jobs")

        except Exception as e:
            print(f"{emp_name}: ERROR: {e}")

    df = pd.DataFrame(rows)

    # Always write output so Actions can upload it
    if df.empty:
        df = pd.DataFrame(columns=["employer","title","location","department","employment_type","url","bucket","reason"])

    df = df.sort_values(["bucket", "employer", "title"], ascending=[True, True, True])
    df.to_csv("jobs_output.csv", index=False)
    print(f"Wrote jobs_output.csv with {len(df)} rows")


if __name__ == "__main__":
    main()
