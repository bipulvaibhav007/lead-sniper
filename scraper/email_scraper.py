# scraper/email_scraper.py
import requests
from bs4 import BeautifulSoup
import re
import csv
import os
from urllib.parse import urlparse
import pandas as pd

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

def extract_emails_from_text(text):
    return list(set(EMAIL_RE.findall(text)))

def scrape_emails_from_website(url, timeout=15):
    try:
        if not url.startswith(("http://", "https://")):
            url = "http://" + url
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/116.0.0.0 Safari/537.36"
            )
        }
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(" ", strip=True)
        emails = extract_emails_from_text(text)
        return emails
    except Exception as e:
        return [f"Error: {e}"]

def filter_rows_without_emails(input_path, output_path, progress_callback=None):
    """
    Reads input CSV, writes ONLY rows that DO NOT have a valid email in any field.
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    email_regex = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}")
    filtered_rows = []

    with open(input_path, newline='', encoding='utf-8') as infile:
        reader = list(csv.DictReader(infile))
        total = len(reader)

        if total == 0:
            raise ValueError("CSV is empty.")

        for idx, row in enumerate(reader):
            row_has_email = False

            # Check all columns for emails
            for value in row.values():
                if value and email_regex.search(str(value)):
                    row_has_email = True
                    break

            # Keep only rows without email
            if not row_has_email:
                filtered_rows.append(row)

            # Update progress
            if progress_callback:
                progress_callback(int(((idx + 1) / total) * 100))

    # Write filtered rows
    if filtered_rows:
        with open(output_path, "w", newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=filtered_rows[0].keys())
            writer.writeheader()
            writer.writerows(filtered_rows)
    else:
        # If no rows were filtered, create an empty file with header
        with open(output_path, "w", newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=reader[0].keys())
            writer.writeheader()

    return output_path
def process_csv_scrape_emails(input_csv, output_csv, progress_callback=None):
    """
    Read CSV (expects a 'website' column or any URL in a column).
    For each row, attempt to get emails from the website and add a column 'found_emails'.
    Save new CSV.
    """
    import pandas as pd
    df = pd.read_csv(input_csv, dtype=str).fillna("")
    out_rows = []

    # Determine website column
    website_col = None
    for c in df.columns:
        if c.lower() in ('website', 'url'):
            website_col = c
            break
    if not website_col:
        # fallback: first column with http/www
        for c in df.columns:
            sample = " ".join(df[c].astype(str).head(5).tolist())
            if any(sub in sample for sub in ("http", "www")):
                website_col = c
                break

    if not website_col:
        raise ValueError("No website/url column found in CSV. Include a 'website' or 'url' column.")

    total = len(df)
    for idx, row in df.iterrows():
        raw_url = str(row.get(website_col, "")).strip()
        if not raw_url or raw_url.upper() == "N/A":
            found = []
        else:
            found = scrape_emails_from_website(raw_url)
            # Filter out errors
            if isinstance(found, list):
                found = [e for e in found if not str(e).startswith("Error")]
            else:
                found = []

        row_dict = row.to_dict()
        row_dict['found_emails'] = ", ".join(found) if found else ""

        out_rows.append(row_dict)

        # Update progress
        if progress_callback:
            progress_callback(int((idx + 1) / total * 100))

    # Write CSV safely
    keys = list(out_rows[0].keys()) if out_rows else list(df.columns) + ['found_emails']
    with open(output_csv, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(out_rows)
