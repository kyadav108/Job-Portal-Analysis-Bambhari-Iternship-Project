# timesjobs_scraper.py
# -*- coding: utf-8 -*-
"""
Combined TimesJobs scraping script (local-machine friendly).
- Scrapes fresher job listing pages (mobile site) for 500 pages by default
- Saves listing URLs to timesjobs_job_urls.csv
- Scrapes details for first 100 fresher URLs (mirrors your original)
- Extracts qualification & job title (with helper functions)
- Extracts skills & description from job description text
- Scrapes IT job listing pages (200 pages by default) and details
- Concatenates fresher + IT datasets and saves final Excel
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import re
import os
import sys
from requests.adapters import HTTPAdapter, Retry
import warnings

# Disable insecure request warnings when verify=False is used.
from requests.packages.urllib3.exceptions import InsecureRequestWarning
warnings.simplefilter('ignore', InsecureRequestWarning)

# --- Configuration (keep these as original defaults; change if you want) ---
FRESHER_PAGES = 500          # original code used range(1, 500)
FRESHER_DETAIL_LIMIT = 100   # original: loop through first 100 job URLs for details
IT_PAGES = 200               # original code scraped 200 pages
OUTPUT_DIR = "."             # change to folder if desired

# Create a requests Session with retries
def new_session():
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s

# --- Part 1: Scrape fresher job listing URLs (mobile site) ---
def scrape_fresher_listing_pages(pages=FRESHER_PAGES):
    base_url = "https://m.timesjobs.com/mobile/jobs-search-result.html?cboWorkExp1=0&sequence={}"
    headers = {"User-Agent": "Mozilla/5.0"}
    session = new_session()

    all_job_urls = []
    for page in range(1, pages + 1):
        url = base_url.format(page)
        print(f"[FRESHER LIST] Scraping page {page} -> {url}")
        try:
            resp = session.get(url, headers=headers, timeout=15, verify=False)
        except Exception as e:
            print(f"  Request error for page {page}: {e}")
            continue

        if resp.status_code != 200:
            print(f"  Failed to fetch page {page} (Status {resp.status_code})")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        job_divs = soup.select("div.srp-listing")

        page_urls = []
        for div in job_divs:
            a_tag = div.find("a", class_="srp-apply-new")
            if a_tag and a_tag.get("href"):
                href = a_tag["href"].replace("&amp;", "&").strip()
                if href.startswith("/"):
                    href = f"https://m.timesjobs.com{href}"
                page_urls.append(href)

        print(f"  Page {page}: {len(page_urls)} URLs found")
        all_job_urls.extend(page_urls)

        # Save progress every 10 pages
        if page % 10 == 0:
            out_path = os.path.join(OUTPUT_DIR, "timesjobs_job_urls.csv")
            pd.DataFrame({"Job_URL": all_job_urls}).to_csv(out_path, index=False)
            print(f"  Saved progress to {out_path}")

        time.sleep(random.uniform(1.5, 3.5))

    # final save
    out_path = os.path.join(OUTPUT_DIR, "timesjobs_job_urls.csv")
    pd.DataFrame({"Job_URL": all_job_urls}).to_csv(out_path, index=False)
    print(f"[FRESHER LIST] Total {len(all_job_urls)} job URLs saved to {out_path}.")
    return all_job_urls

# --- Part 2: Scrape details for fresher job URLs (first 100) ---
def scrape_fresher_details(listing_csv="timesjobs_job_urls.csv", limit=FRESHER_DETAIL_LIMIT):
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")
    }
    session = new_session()

    # Load the listing CSV
    if not os.path.exists(listing_csv):
        print(f"Listing file {listing_csv} not found. Exiting fresher details scraping.")
        return pd.DataFrame()

    all_urls = pd.read_csv(listing_csv)
    urls_to_scrape = all_urls["Job_URL"].dropna().tolist()[:limit]

    all_data = []
    for i, url in enumerate(urls_to_scrape, start=1):
        print(f"[FRESHER DETAILS] Scraping ({i}/{len(urls_to_scrape)}): {url}")
        try:
            resp = session.get(url, headers=headers, timeout=15, verify=False)
            if resp.status_code != 200:
                print(f"  Skipping (status {resp.status_code})")
                continue
            soup = BeautifulSoup(resp.text, "html.parser")

            # Company Name
            company_tag = soup.find("h2")
            company = None
            if company_tag:
                # Sometimes <h2><span>CompanyName</span></h2>
                span = company_tag.find("span")
                if span:
                    company = span.get_text(strip=True)
                else:
                    company = company_tag.get_text(strip=True)

            # Posting time (mobile markup may differ)
            posting_time = None
            posting_time_tag = soup.find("span", class_="posting-time")
            if posting_time_tag:
                posting_time = posting_time_tag.get_text(strip=True)

            # Location
            location_tag = soup.find("div", class_="srp-loc")
            location = location_tag.text.replace("Location:", "").strip() if location_tag else None

            # Experience
            experience_tag = soup.find("div", class_="srp-exp")
            experience = re.sub(r"\s+", " ", experience_tag.get_text()).strip() if experience_tag else None

            # Salary
            salary_tag = soup.find("div", class_="srp-sal")
            salary = salary_tag.text.strip() if salary_tag else None

            # Job Role / Description
            job_role_tag = soup.find("div", id="JobDescription")
            job_role = re.sub(r"\s+", " ", job_role_tag.get_text()).strip() if job_role_tag else None

            # Industry Type
            industry_tag = soup.find("span", class_="jd-cont-bx")
            industry = industry_tag.get_text(strip=True) if industry_tag else None

            # Qualification: try multiple strategies
            qualification = None
            # look for li.clearfix label containing 'Qualification'
            qualification_li = None
            for li in soup.find_all("li", class_="clearfix"):
                label = li.find("label")
                if label and "Qualification" in label.get_text():
                    qualification_li = li
                    break
            if qualification_li:
                span = qualification_li.find("span", class_="jd-cont-bx") or qualification_li.find("span", class_="basic-info-dtl")
                if span:
                    qualification = re.sub(r"\s+", " ", span.get_text()).strip()

            # Employment Type
            emp_type = None
            emp_tag = soup.find("label", string=lambda text: text and "Employment Type" in text)
            if emp_tag:
                span = emp_tag.find_next("span", class_="jd-cont-bx")
                if span:
                    emp_type = span.get_text(strip=True)

            all_data.append({
                "URL": url,
                "Company": company,
                "Posting_Time": posting_time,
                "Location": location,
                "Experience": experience,
                "Salary": salary,
                "Job_Description": job_role,
                "Industry": industry,
                "Qualification": qualification,
                "Employment_Type": emp_type
            })

            print(f"  Done: {company or 'N/A'}")
            time.sleep(random.uniform(2, 5))

        except Exception as e:
            print(f"  Error scraping {url}: {e}")
            continue

    df_freshers = pd.DataFrame(all_data)
    out_path = os.path.join(OUTPUT_DIR, "timesjobs_job_details.csv")
    df_freshers.to_csv(out_path, index=False)
    print(f"[FRESHER DETAILS] Saved {len(df_freshers)} records to {out_path}")
    return df_freshers

# --- Helper: function to extract Qualification from a detail URL (if missing) ---
def get_qualification(url, session=None):
    if session is None:
        session = new_session()
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = session.get(url, headers=headers, timeout=12, verify=False)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        label = soup.find("label", string=lambda t: t and "Qualification" in t)
        if label:
            span = label.find_next("span", class_="jd-cont-bx") or label.find_next("span", class_="basic-info-dtl")
            if span:
                return re.sub(r"\s+", " ", span.get_text(strip=True)).strip()
        # try the li.clearfix strategy
        for li in soup.find_all("li", class_="clearfix"):
            lab = li.find("label")
            if lab and "Qualification" in lab.get_text():
                span = li.find("span", class_="jd-cont-bx") or li.find("span", class_="basic-info-dtl")
                if span:
                    return re.sub(r"\s+", " ", span.get_text(strip=True)).strip()
        return None
    except Exception as e:
        print(f"  get_qualification error for {url}: {e}")
        return None

# --- Helper: function to extract Job Title (h1) ---
def get_job_title(url, session=None):
    if session is None:
        session = new_session()
    headers = {"User-Agent": "Mozilla/5.0 (compatible; JobScraperBot/1.0)"}
    try:
        resp = session.get(url, headers=headers, timeout=12, verify=False)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        title_tag = soup.find("h1")
        if title_tag:
            return title_tag.get_text(strip=True)
        # fallback: find h1-like job title markup
        title_alt = soup.find("h1", class_="jd-job-title")
        if title_alt:
            return title_alt.get_text(strip=True)
        return None
    except Exception as e:
        print(f"  get_job_title error for {url}: {e}")
        return None

# --- Part 3: Post-process fresher dataframe: fill Qualification and add Job_Title ---
def postprocess_freshers(df_freshers):
    session = new_session()

    # fill missing Qualification
    print("[POSTPROCESS] Filling missing Qualification entries...")
    for idx, row in df_freshers.iterrows():
        if pd.isna(row.get("Qualification")) or not row.get("Qualification"):
            qual = get_qualification(row["URL"], session=session)
            df_freshers.at[idx, "Qualification"] = qual
            time.sleep(random.uniform(2, 5))

    # get job titles for each URL
    print("[POSTPROCESS] Extracting Job_Title for each fresher URL...")
    job_titles = []
    for i, url in enumerate(df_freshers["URL"].tolist(), start=1):
        print(f"  [{i}/{len(df_freshers)}] fetching title for {url}")
        title = get_job_title(url, session=session)
        job_titles.append(title)
        time.sleep(random.uniform(2, 5))

    df_freshers["Job_Title"] = job_titles
    out_path = os.path.join(OUTPUT_DIR, "jobs_freshers.csv")
    df_freshers.to_csv(out_path, index=False)
    print(f"[POSTPROCESS] Saved updated fresher file to {out_path}")
    return df_freshers

# --- Part 4: Extract skills & short job description from Job_Description text ---
def extract_job_info_from_description(df):
    print("[EXTRACT] Parsing Job_Description for Skills and Description...")
    def extract_job_info(text):
        if not isinstance(text, str):
            return {"Skills": None, "Description": None}
        text_clean = re.sub(r'\s+', ' ', text.strip())
        description = None
        match_desc = re.search(r'Job Responsibilities(.*?)(Education Requirement|Skills|Skills & Competencies|$)',
                               text_clean, flags=re.IGNORECASE)
        if match_desc:
            description = match_desc.group(1).strip()

        skills = None
        match_skills = re.search(r'(Skills & Competencies|Skills)(.*)', text_clean, flags=re.IGNORECASE)
        if match_skills:
            skills = match_skills.group(2).strip()
        return {"Skills": skills, "Description": description}

    extracted = df["Job_Description"].apply(extract_job_info)
    df["Skills"] = extracted.apply(lambda x: x["Skills"])
    df["Description"] = extracted.apply(lambda x: x["Description"])
    out_path = os.path.join(OUTPUT_DIR, "jobs_freshers_parsed.csv")
    df.to_csv(out_path, index=False)
    print(f"[EXTRACT] Saved parsed fresher data to {out_path}")
    return df

# --- Part 5: Scrape IT job listing URLs (desktop site) ---
def get_it_job_urls(base_url, num_pages=IT_PAGES):
    headers = {"User-Agent": "Mozilla/5.0 (compatible; JobScraperBot/1.0)"}
    session = new_session()
    job_links = []

    for page in range(1, num_pages + 1):
        url = f"{base_url}&sequence={page}"
        print(f"[IT LIST] Scraping page {page} -> {url}")
        try:
            resp = session.get(url, headers=headers, timeout=15, verify=False)
            if resp.status_code != 200:
                print(f"  Failed to fetch page {page} (Status {resp.status_code})")
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            job_boxes = soup.find_all("li", class_="clearfix job-bx wht-shd-bx")
            found_on_page = 0
            for job in job_boxes:
                a_tag = job.find("a", href=True, class_="posoverlay_srp")
                if a_tag:
                    job_links.append(a_tag["href"])
                    found_on_page += 1
            print(f"  Found {found_on_page} job cards on page {page}.")
            time.sleep(random.uniform(2, 6))
        except Exception as e:
            print(f"  Error on page {page}: {e}")
            continue

    out_path = os.path.join(OUTPUT_DIR, "timesjobs_ITjob_URL.csv")
    pd.DataFrame({"URL": job_links}).to_csv(out_path, index=False)
    print(f"[IT LIST] Saved {len(job_links)} IT job URLs to {out_path}")
    return job_links

# --- Part 6: Scrape IT job details from collected URLs ---
def scrape_it_job_details(it_urls_csv="timesjobs_ITjob_URL.csv"):
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    }
    session = new_session()

    if not os.path.exists(it_urls_csv):
        print(f"IT URL file {it_urls_csv} not found. Exiting IT details scraping.")
        return pd.DataFrame()

    df_urls = pd.read_csv(it_urls_csv)
    urls = df_urls['URL'].dropna().tolist()

    data = []
    for i, url in enumerate(urls, start=1):
        try:
            print(f"[IT DETAILS] Scraping ({i}/{len(urls)}): {url}")
            resp = session.get(url, headers=headers, timeout=15, verify=False)
            if resp.status_code != 200:
                print(f"  Skipping (status {resp.status_code})")
                continue
            soup = BeautifulSoup(resp.text, "html.parser")

            # Job title
            title_tag = soup.find("h1", class_="jd-job-title")
            title = title_tag.get_text(strip=True) if title_tag else None

            # Company
            company_tag = soup.find("h2")
            company = company_tag.get_text(strip=True) if company_tag else None

            # Posting date
            posted = None
            posted_tag = soup.find("span", class_="posted-days")
            if posted_tag:
                posted = posted_tag.get_text(strip=True)

            # Location
            location = None
            loc_i = soup.find("i", class_="location")
            if loc_i:
                parent_li = loc_i.find_parent("li")
                if parent_li:
                    location = re.sub(r"\s+", " ", parent_li.get_text(separator=" ", strip=True)).strip()

            # Experience
            experience = None
            exp_i = soup.find("i", class_="experience")
            if exp_i:
                parent_li = exp_i.find_parent("li")
                if parent_li:
                    experience = re.sub(r"\s+", " ", parent_li.get_text(separator=" ", strip=True)).strip()

            # Salary
            salary = None
            sal_i = soup.find("i", class_="salary")
            if sal_i:
                parent_li = sal_i.find_parent("li")
                if parent_li:
                    salary = re.sub(r"\s+", " ", parent_li.get_text(separator=" ", strip=True)).strip()

            # Industry, Qualification, Employment Type
            industry = None
            qualification = None
            employment_type = None

            for li in soup.find_all("li", class_="clearfix"):
                label = li.find("label")
                if not label:
                    continue
                label_text = label.get_text()
                if "Industry" in label_text:
                    span = li.find("span", class_="basic-info-dtl")
                    if span:
                        industry = re.sub(r"\s+", " ", span.get_text(strip=True)).strip()
                if "Qualification" in label_text:
                    span = li.find("span", class_="basic-info-dtl")
                    if span:
                        qualification = re.sub(r"\s+", " ", span.get_text(strip=True)).strip()

            # Employment type possibility
            employment_tag = soup.find("span", class_="mt-4")
            if employment_tag:
                employment_type = re.sub(r"\s+", " ", employment_tag.get_text(strip=True)).strip()

            # Skills tags
            skills = []
            for a in soup.select("span.jd-skill-tag a"):
                if a.get("title"):
                    skills.append(a.get("title").replace(" Jobs", ""))
            skills_text = ", ".join(skills) if skills else None

            # Description
            desc_div = soup.find("div", class_="jd-desc")
            description = desc_div.get_text(" ", strip=True) if desc_div else None

            data.append({
                "URL": url,
                "Job_Title": title,
                "Company": company,
                "Posting_Date": posted,
                "Location": location,
                "Experience": experience,
                "Salary": salary,
                "Industry": industry,
                "Qualification": qualification,
                "Employment_Type": employment_type,
                "Skills": skills_text,
                "Description": description
            })

            sleep_time = random.uniform(2, 6)
            print(f"  Sleeping for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)

        except Exception as e:
            print(f"  Error scraping {url}: {e}")
            continue

    df_it = pd.DataFrame(data)
    out_path = os.path.join(OUTPUT_DIR, "timesjobs_ITjobs.csv")
    df_it.to_csv(out_path, index=False)
    print(f"[IT DETAILS] Saved {len(df_it)} IT records to {out_path}")
    return df_it

# --- Part 7: Final concatenation and saving to Excel ---
def finalize_and_save(df_freshers, df_it):
    # Drop the columns specified in your original script
    if not df_freshers.empty:
        if "Posting_Time" in df_freshers.columns:
            df_freshers.drop(columns=["Posting_Time"], inplace=True, errors='ignore')
        for col in ["Qualification", "Employment_Type"]:
            if col in df_freshers.columns:
                df_freshers.drop(columns=[col], inplace=True, errors='ignore')

    if not df_it.empty:
        for col in ["Posting_Date", "Qualification", "Employment_Type"]:
            if col in df_it.columns:
                df_it.drop(columns=[col], inplace=True, errors='ignore')

    # Concatenate (keep original order of columns as you requested)
    timesjobs = pd.concat([df_freshers, df_it], ignore_index=True, sort=False)

    # Desired column order (from your original script)
    columns_wanted = ["Company", "Industry", "Job_Title", "Experience", "Salary", "Location", "Description", "Skills", "URL"]
    # Retain only available columns but keep order
    cols_present = [c for c in columns_wanted if c in timesjobs.columns]
    timejobs_data = timesjobs[cols_present]

    out_path = os.path.join(OUTPUT_DIR, "Timesjobs_data.xlsx")
    timejobs_data.to_excel(out_path, index=False)
    print(f"[FINAL] Saved combined data to {out_path}")

# --- main flow ---
def main():
    # 1) Scrape fresher listing pages (500 pages)
    listing_file = os.path.join(OUTPUT_DIR, "timesjobs_job_urls.csv")
    if not os.path.exists(listing_file):
        scrape_fresher_listing_pages(FRESHER_PAGES)
    else:
        print(f"[MAIN] Found existing listing file {listing_file}, skipping listing scrape (delete file to re-run).")

    # 2) Scrape fresher details for first 100 URLs
    details_file = os.path.join(OUTPUT_DIR, "timesjobs_job_details.csv")
    if not os.path.exists(details_file):
        df_freshers = scrape_fresher_details(listing_csv=listing_file, limit=FRESHER_DETAIL_LIMIT)
    else:
        print(f"[MAIN] Found existing fresher details file {details_file}, loading it.")
        df_freshers = pd.read_csv(details_file)

    # 3) Postprocess: fill qualification and extract job titles (if not already done)
    # We'll only run if Job_Title is missing or Qualification missing in df_freshers
    needs_post = True
    if "Job_Title" in df_freshers.columns and df_freshers["Job_Title"].notna().all():
        # if all titles present and qualification present, skip
        if "Qualification" in df_freshers.columns and df_freshers["Qualification"].notna().all():
            needs_post = False

    if needs_post:
        df_freshers = postprocess_freshers(df_freshers)

    # 4) Extract Skills & Description from Job_Description text
    df_freshers = extract_job_info_from_description(df_freshers)

    # 5) Drop columns Posting_Time, Qualification, Employment_Type from fresher df (to mimic original)
    for col in ["Posting_Time", "Qualification", "Employment_Type"]:
        if col in df_freshers.columns:
            df_freshers.drop(columns=[col], inplace=True, errors='ignore')

    # Save intermediate final fresher file
    fresher_final_path = os.path.join(OUTPUT_DIR, "jobs_freshers_final.csv")
    df_freshers.to_csv(fresher_final_path, index=False)
    print(f"[MAIN] Saved fresher final CSV to {fresher_final_path}")

    # 6) Scrape IT job listing pages (200 pages) - unless the file exists
    it_listing_file = os.path.join(OUTPUT_DIR, "timesjobs_ITjob_URL.csv")
    if not os.path.exists(it_listing_file):
        base_it_url = ("https://www.timesjobs.com/candidate/job-search.html?"
                       "searchType=personalizedSearch&from=submit&txtKeywords=software+engineer&txtLocation=")
        get_it_job_urls(base_it_url, num_pages=IT_PAGES)
    else:
        print(f"[MAIN] Found existing IT listing file {it_listing_file}, skipping IT listing scrape.")

    # 7) Scrape IT job details (from saved IT listing CSV)
    it_details_file = os.path.join(OUTPUT_DIR, "timesjobs_ITjobs.csv")
    if not os.path.exists(it_details_file):
        df_it = scrape_it_job_details(it_urls_csv=it_listing_file)
    else:
        print(f"[MAIN] Found existing IT details file {it_details_file}, loading it.")
        df_it = pd.read_csv(it_details_file)

    # 8) Final concatenation & save to Excel
    finalize_and_save(df_freshers, df_it)

    print("[MAIN] All done.")

if __name__ == "__main__":
    main()
