import requests
from bs4 import BeautifulSoup
from config.db_connection import connect
import pandas as pd
import time
import random
from datetime import datetime
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def parse_sections(soup):
    data = {}
    for item in soup.select("div.job-description__item"):
        h3 = item.select_one("h3")
        content_div = item.select_one(".job-description__item--content")
        if not (h3 and content_div):
            continue
        title = h3.get_text(strip=True)
        blocks = []
        for li in content_div.select("li"):
            blocks.append(li.get_text(strip=True))
        if not blocks:
            for p in content_div.select("p"):
                text = p.get_text(strip=True)
                if text:
                    blocks.append(text)
        if not blocks:
            text = content_div.get_text("\n", strip=True)
            if text:
                blocks.append(text)
        data[title] = blocks
    return data


def is_valid_job_data(job_data):
    if not job_data:
        return False
    important_fields = [
        job_data.get("company_name"),
        job_data.get("job_name"),
        job_data.get("job_detail"),
        job_data.get("candidate_requirements")
    ]
    if all(field is None or field == [] for field in important_fields):
        return False
    return True


def parse_job_detail(url):
    try:
        resp = requests.get(url, timeout=15)
        status_code = resp.status_code
        if status_code != 200:
            return (status_code, None)
    except requests.exceptions.Timeout:
        print(f"Timeout")
        return (None, None)
    except requests.exceptions.RequestException as e:
        print(f"Request error: {str(e)[:50]}")
        return (None, None)
    
    # Delay dài hơn để tránh lại 429
    time.sleep(random.uniform(5, 8))
    
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Company
    company_tag = soup.select_one("div.company-name-label a.name")
    company_name = company_tag.get_text(strip=True) if company_tag else None
    company_links = company_tag["href"] if company_tag and company_tag.has_attr("href") else None
    
    # Job name
    job_name_tags = soup.select("h1.job-detail__info--title a")
    job_name = " ".join(tag.get_text(strip=True) for tag in job_name_tags) if job_name_tags else None
    
    # Tags
    tags = [a.get_text(strip=True) for a in soup.select("div.job-tags a")]
    
    # Job detail sections
    sections = parse_sections(soup)
    
    # General info
    general_info = {
        "level": None,
        "education": None,
        "quantity": None,
        "form_of_work": None,
    }
    
    for row in soup.select(".job-detail__body-right--box-general .box-general-group"):
        label = row.select_one(".box-general-group-info-title")
        value = row.select_one(".box-general-group-info-value")
        if label and value:
            text_label = label.get_text(strip=True)
            text_value = value.get_text(strip=True)
            if text_label == "Cấp bậc":
                general_info["level"] = text_value
            elif text_label == "Học vấn":
                general_info["education"] = text_value
            elif text_label == "Số lượng tuyển":
                general_info["quantity"] = text_value
            elif text_label == "Hình thức làm việc":
                general_info["form_of_work"] = text_value
    
    # Deadline
    deadline_tag = soup.select_one("div.job-detail__information-detail--actions-label")
    application_deadline = None
    if deadline_tag:
        text = deadline_tag.get_text(strip=True)
        if "Hạn nộp hồ sơ" in text:
            application_deadline = text.replace("Hạn nộp hồ sơ:", "").strip()
    
    job_data = {
        "company_name": company_name,
        "company_links": company_links,
        "job_name": job_name,
        "post_link": str(url),
        "tags": tags,
        "job_detail": sections.get("Mô tả công việc"),
        "candidate_requirements": sections.get("Yêu cầu ứng viên"),
        "interest": sections.get("Quyền lợi"),
        "location": sections.get("Địa điểm làm việc"),
        "working_time": sections.get("Thời gian làm việc"),
        "general_info": general_info,
        "application_deadline": application_deadline,
    }
    
    return (200, job_data)


if __name__ == "__main__":
    file_429 = "data/errors/429_rate_limit.csv"
    if not os.path.exists(file_429):
        print(f"Không tìm thấy file {file_429}")
        exit(1)
    
    # Đọc các link bị 429
    df = pd.read_csv(file_429)
    if 'url' not in df.columns:
        print("File không có cột 'url'")
        exit(1)
    
    links_429 = df['url'].dropna().tolist()
    print(f"Tìm thấy {len(links_429)} links bị 429\n")
    
    if not links_429:
        print("Không có link nào cần retry!")
        exit(0)
    
    # Kết nối MongoDB
    db, collection = connect()
    if db is None or collection is None:
        print("Không thể kết nối MongoDB")
        exit(1)
    
    # Check số jobs hiện tại
    current_jobs = collection.count_documents({})
    print(f"Số jobs trong DB hiện tại: {current_jobs}\n")
    print(f"Thời gian ước tính: {len(links_429) * 6 / 60:.1f} phút\n")
    
    success = 0
    still_429 = 0
    invalid = 0
    duplicates = 0
    other_errors = 0
    
    start_time = time.time()
    
    for i, url in enumerate(links_429, 1):
        print(f"\n[{i}/{len(links_429)}] {url[:70]}")
        
        status_code, job_data = parse_job_detail(url)
        
        if status_code == 200 and is_valid_job_data(job_data):
            # Check trùng
            if not collection.find_one({"post_link": job_data["post_link"]}):
                collection.insert_one(job_data)
                success += 1
            else:
                duplicates += 1
                print(f"Duplicate, skipped")
        elif status_code == 429:
            still_429 += 1
            print(f"Vẫn bị 429, nghỉ thêm 60s...")
            time.sleep(60)
        elif status_code == 200 and not is_valid_job_data(job_data):
            invalid += 1
            print(f"Invalid data (all null)")
        else:
            other_errors += 1
            print(f"Error {status_code}")
    

    if still_429 > 0:
        print(f"\nLƯU Ý: Còn {still_429} links vẫn bị 429")
        print(f"Có thể chạy lại script này sau 1-2 giờ nữa")