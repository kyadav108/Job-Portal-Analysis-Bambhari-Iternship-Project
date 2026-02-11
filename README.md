📊 Job Market Analysis – Web Scraping & Data Processing
🔹 Overview

This project analyzes the job market using 160K+ real job postings scraped from Naukri.com and TimesJobs.
The objective is to uncover hiring patterns, skill demand, salary trends, and experience requirements through large-scale data processing and rule-based text analysis.

🎯 Objectives

Analyze hiring trends across industries

Identify in-demand skills

Study experience-level distribution

Normalize and evaluate salary patterns

Prepare structured datasets for visualization & analytics

🌐 Data Sources

Job postings scraped from:

Naukri.com

TimesJobs

Extracted Fields

Job Title

Company Name

Location

Experience Required

Salary

Skills / Tags

Industry

Job Description

Qualification

Employment Type

⚙️ Methodology
✅ 1. Data Collection

Web scraping using Python & BeautifulSoup

Pagination-based crawling

Randomized delays to avoid blocking

✅ 2. Data Cleaning & Preprocessing

Removed duplicate records

Handled missing values

Standardized inconsistent fields

Normalized text data

✅ 3. Feature Engineering

Created analytical features including:

✔ Industry Binning
✔ Experience Parsing & Categorization
✔ Salary Normalization (LPA conversion)

✅ 4. Text Processing (Rule-Based)

Extracted structured insights from job descriptions using:

Regex patterns

Keyword mapping

Rule-based text classification

⚠ Note:
This project does NOT use NLP models or machine learning-based text processing.
All text extraction is performed via rule-based logic.

✅ 5. High-Cardinality Reduction

Applied binning & grouping techniques to manage:

Industry labels

Job titles

Skills / Tags

🛠 Tech Stack

Python

BeautifulSoup (Web Scraping)

Pandas (Data Processing)

NumPy (Numerical Operations)

Regex (Text Extraction)

Excel / Tablue 

📊 Dataset Summary

Initial Records: 163,188

Final Cleaned Records: 149,274

📈 Key Analyses Supported

This dataset enables:

Skill demand analysis

Salary benchmarking

Industry hiring comparison

Experience distribution insights

Geographic job trends

🚧 Limitations

Salary frequently not disclosed

Skill tags inconsistent across postings

Highly fragmented job titles

Scraped data subject to platform bias

🔮 Future Enhancements

Skill clustering automation

Salary prediction modeling

Trend analysis over time

Dashboard & KPI reporting

⭐ Project Value

Transforms raw job postings into structured market intelligence through large-scale scraping, preprocessing, and rule-based text analytics.
