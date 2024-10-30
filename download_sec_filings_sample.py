import random
from datetime import datetime
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time


download_folder_path = ""

def fetch_all_ciks_by_sic(sic_code, headers=None):
    """
    Fetch all CIKs for a given SIC code by iterating through pages on EDGAR.
    """
    edgar_base_url = "https://www.sec.gov/cgi-bin/browse-edgar"
    all_ciks = []
    start = 0

    while True:
        params = {
            "action": "getcompany",
            "SIC": sic_code,
            "owner": "exclude",
            "count": 100,
            "start": start
        }

        response = requests.get(edgar_base_url, params=params, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        filings_table = soup.find("table", class_="tableFile2")

        if not filings_table:
            print(f"No more companies found for SIC code {sic_code}.")
            break

        rows = filings_table.find_all("tr")[1:]
        new_ciks = 0

        for row in rows:
            cells = row.find_all("td")
            if cells and cells[0].find("a"):
                cik_url = cells[0].find("a")["href"]
                cik = cik_url.split("CIK=")[-1].split("&")[0]
                if cik not in all_ciks:
                    all_ciks.append(cik)
                    new_ciks += 1

        print(f"Found {new_ciks} new CIKs on page starting at {start}. Total so far: {len(all_ciks)}")

        if new_ciks < 100:
            break

        start += 100
        time.sleep(0.5)

    return all_ciks if all_ciks else []


def is_folder_complete(company_folder, expected_file_count=2, min_file_size=100):
    files = os.listdir(company_folder)
    if len(files) < expected_file_count:
        return False

    for file_name in files:
        file_path = os.path.join(company_folder, file_name)
        if os.path.getsize(file_path) < min_file_size:
            return False

    return True


def fetch_and_save_annual_reports(ciks, download_folder="./sic_filings", num_filings=10, start_year=2000,
                                  end_year=datetime.now().year, log_file="debug_log.txt"):
    """
    Fetch and save all annual reports (10-K, 10-K/A, 20-F, 40-F, etc.) for a list of CIKs within a specified date range.
    """
    edgar_base_url = "https://www.sec.gov/cgi-bin/browse-edgar"
    headers = {"User-Agent": "Your Company Contact@yourdomain.com"}
    os.makedirs(download_folder, exist_ok=True)

    annual_report_types = ["10-K", "10-K/A", "20-F", "40-F"]

    with open(log_file, "w", encoding="utf-8") as log:
        for cik in ciks:
            company_folder = os.path.join(download_folder, cik)
            os.makedirs(company_folder, exist_ok=True)

            if is_folder_complete(company_folder):
                log.write(f"Skipping CIK {cik} as filings are already complete.\n")
                continue

            for report_type in annual_report_types:
                log.write(f"\nFetching {report_type} filings for CIK: {cik}\n")

                params = {
                    "action": "getcompany",
                    "CIK": cik,
                    "type": report_type,
                    "count": num_filings,
                    "owner": "exclude"
                }

                try:
                    response = requests.get(edgar_base_url, params=params, headers=headers)
                    response.raise_for_status()
                except requests.RequestException as e:
                    log.write(f"Error fetching {report_type} filings for CIK {cik}: {e}\n")
                    continue

                soup = BeautifulSoup(response.text, 'html.parser')
                filings_table = soup.find("table", class_="tableFile2")

                if not filings_table:
                    log.write(f"No filings table found for CIK: {cik}, report type: {report_type}\n")
                    continue

                rows = filings_table.find_all("tr")[1:]

                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) > 1 and cells[1].find("a"):
                        filing_date_str = cells[3].text.strip()
                        filing_date = datetime.strptime(filing_date_str, "%Y-%m-%d")

                        if filing_date.year < start_year or filing_date.year > end_year:
                            log.write(
                                f"Skipping {report_type} filing for CIK {cik} on {filing_date_str}, outside of date range.\n")
                            continue

                        filing_href = cells[1].find("a")["href"]
                        filing_url = urljoin("https://www.sec.gov", filing_href)

                        log.write(
                            f"Filing URL for CIK {cik}, report type {report_type}, date {filing_date_str}: {filing_url}\n")

                        try:
                            filing_page = requests.get(filing_url, headers=headers)
                            filing_page.raise_for_status()
                        except requests.RequestException as e:
                            log.write(f"Error fetching filing page for CIK {cik}, report type {report_type}: {e}\n")
                            continue

                        try:
                            filing_soup = BeautifulSoup(filing_page.content, 'html.parser')
                            documents_table = filing_soup.find("table", class_="tableFile")

                            if not documents_table:
                                log.write(
                                    f"No documents table found at {filing_url} for CIK {cik}, report type {report_type}\n")
                                continue

                            for doc_row in documents_table.find_all("tr"):
                                doc_cells = doc_row.find_all("td")
                                if len(doc_cells) > 3:
                                    description = doc_cells[1].text.strip().upper()
                                    if any(typ in description for typ in
                                           annual_report_types) or "COMPLETE SUBMISSION TEXT FILE" in description:
                                        doc_href = doc_cells[2].find("a")["href"]
                                        document_link = urljoin("https://www.sec.gov", doc_href)
                                        doc_name = document_link.split("/")[-1]

                                        log.write(
                                            f"Document link found for CIK {cik}, report type {report_type}, date {filing_date_str}: {document_link}, Description: {description}\n")

                                        try:
                                            doc_response = requests.get(document_link, headers=headers)
                                            doc_response.raise_for_status()

                                            file_extension = doc_name.split(".")[-1] if "." in doc_name else "html"
                                            clean_doc_name = os.path.basename(doc_name).split(".")[0]
                                            file_path = os.path.join(company_folder,
                                                                     f"{clean_doc_name}.{file_extension}")

                                            log.write(
                                                f"Saving file for CIK {cik}, report type {report_type}, date {filing_date_str} at path: {file_path}\n")

                                            if file_extension in ["html", "txt"]:
                                                try:
                                                    doc_text = BeautifulSoup(doc_response.content,
                                                                             "html.parser").get_text()
                                                    with open(file_path, "w", encoding="utf-8") as text_file:
                                                        text_file.write(doc_text)
                                                    log.write(
                                                        f"Saved text file for CIK {cik}, report type {report_type} at {file_path}\n")
                                                except Exception as e:
                                                    log.write(f"Error parsing text for CIK {cik} at {file_path}: {e}\n")
                                                    continue

                                            elif file_extension in ["xbrl", "xml"]:
                                                with open(file_path, "wb") as xml_file:
                                                    xml_file.write(doc_response.content)
                                                log.write(
                                                    f"Saved XML file for CIK {cik}, report type {report_type} at {file_path}\n")

                                            else:
                                                with open(file_path, "wb") as other_file:
                                                    other_file.write(doc_response.content)
                                                log.write(
                                                    f"Saved file for CIK {cik}, report type {report_type} at {file_path}\n")

                                        except requests.RequestException as e:
                                            log.write(
                                                f"Error downloading document for CIK {cik}, report type {report_type}: {e}\n")

                        except Exception as e:
                            log.write(f"Failed to parse filing page for CIK {cik}, report type {report_type}: {e}\n")

                        time.sleep(1)


def fetch_annual_reports_by_sic_code(sic_code, download_folder="./sic_filings", num_filings=10, start_year=2000,
                                     end_year=datetime.now().year, sample_size=None):
    headers = {"User-Agent": "Your Company Contact@yourdomain.com"}
    ciks = fetch_all_ciks_by_sic(sic_code, headers=headers)

    if sample_size is not None and sample_size < len(ciks):
        ciks = random.sample(ciks, sample_size)
        print(f"Randomly selected {sample_size} CIKs out of {len(ciks)} total CIKs.")
    else:
        print(f"Using all {len(ciks)} CIKs.")

    if not ciks:
        print(f"No CIKs found for SIC code {sic_code}.")
        return

    fetch_and_save_annual_reports(
        ciks,
        download_folder=os.path.join(download_folder, str(sic_code)),
        num_filings=num_filings,
        start_year=start_year,
        end_year=end_year
    )


# Example usage with a random sample of 50 CIKs
fetch_annual_reports_by_sic_code(
    sic_code="7311",
    download_folder=download_folder_path,
    num_filings=10,
    start_year=2018,
    end_year=2023,
    sample_size=3
)
