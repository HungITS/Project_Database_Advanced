from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from config.db_connection import connect
from datetime import datetime
import os


def parse_sections(soup):
    """Parse các section có tiêu đề h3 trong phần mô tả công việc"""
    data = {}

    for item in soup.select("div.job-description__item"):
        h3 = item.select_one("h3")
        content_div = item.select_one(".job-description__item--content")
        if not (h3 and content_div):
            continue

        title = h3.get_text(strip=True)

        blocks = []
        # Ưu tiên lấy <li>
        for li in content_div.select("li"):
            blocks.append(li.get_text(strip=True))

        # Nếu không có li thì lấy <p>
        if not blocks:
            for p in content_div.select("p"):
                text = p.get_text(strip=True)
                if text:
                    blocks.append(text)

        # Nếu vẫn không có thì fallback lấy toàn bộ text
        if not blocks:
            text = content_div.get_text("\n", strip=True)
            if text:
                blocks.append(text)

        data[title] = blocks

    return data


def is_valid_job_data(job_data):
    """Kiểm tra xem dữ liệu job có hợp lệ không (không phải toàn null)"""
    if not job_data:
        return False
    
    # Các trường quan trọng cần kiểm tra
    important_fields = [
        job_data.get("company_name"),
        job_data.get("job_name"),
        job_data.get("job_detail"),
        job_data.get("candidate_requirements")
    ]
    
    # Nếu tất cả các trường quan trọng đều None/rỗng -> dữ liệu không hợp lệ
    if all(field is None or field == [] for field in important_fields):
        return False
    
    return True


def parse_job_detail(url):
    """
    Request job page và parse dữ liệu cần thiết
    Returns: tuple (status_code, job_data)
        - status_code: 200, 404, 429, hoặc các mã khác
        - job_data: dict chứa thông tin job hoặc None
    """
    try:
        resp = requests.get(url, timeout=10)
        status_code = resp.status_code
        
        # Nếu không phải 200, trả về status code và None
        if status_code != 200:
            return (status_code, None)
        
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi request {url}: {e}")
        return (None, None)
    
    time.sleep(random.uniform(0.5, 2))

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

    # General info mặc định
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


def save_error_links(links, filename):
    """Lưu các link lỗi vào file CSV"""
    if not links:
        return
    
    df = pd.DataFrame(links)
    
    # Tạo thư mục nếu chưa có
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    # Append nếu file đã tồn tại, ngược lại tạo mới
    if os.path.exists(filename):
        df.to_csv(filename, mode='a', header=False, index=False)
    else:
        df.to_csv(filename, index=False)
    
    print(f"Đã lưu {len(links)} link vào {filename}")


def batch_links(lst, batch_size):
    """Chia list thành các batch nhỏ"""
    for i in range(0, len(lst), batch_size):
        yield lst[i:i + batch_size]


if __name__ == "__main__":
    # Kết nối MongoDB
    db, collection = connect()
    if db is None or collection is None:
        print("Unable to connect to MongoDB, exiting...")
        exit(1)

    # Đọc links.csv (chỉ 1 cột duy nhất)
    df = pd.read_csv("data/raw/links.csv")
    links = df.iloc[:, 0].dropna().tolist()

    links = [url for url in links if isinstance(url, str) and url.startswith("https://www.topcv.vn/viec-lam")]

    if not links:
        print("Không có link hợp lệ để crawl, thoát...")
        exit(1)

    # Khởi tạo danh sách lưu các link lỗi
    error_404_links = []  # Link không tồn tại
    error_429_links = []  # Link bị rate limit
    invalid_data_links = []  # Link có dữ liệu toàn null

    batch_size = 200
    max_workers = 10
    
    # Thống kê
    total_inserted = 0
    total_skipped = 0
    total_404 = 0
    total_429 = 0
    total_invalid = 0

    for batch_num, batch in enumerate(batch_links(links, batch_size), start=1):
        print(f"\n{'='*60}")
        print(f"==== Batch {batch_num}, number of links: {len(batch)} ====")
        print(f"{'='*60}")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(parse_job_detail, url): url for url in batch}

            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    status_code, job_data = future.result()
                    
                    # Xử lý lỗi 404
                    if status_code == 404:
                        error_404_links.append({"url": url, "timestamp": datetime.now().isoformat()})
                        total_404 += 1
                        print(f"404 Not Found: {url}")
                        continue
                    
                    # Xử lý lỗi 429 (rate limit)
                    elif status_code == 429:
                        error_429_links.append({"url": url, "timestamp": datetime.now().isoformat()})
                        total_429 += 1
                        print(f"429 Rate Limited: {url}")
                        continue
                    
                    # Xử lý các lỗi khác
                    elif status_code != 200 or job_data is None:
                        print(f"Error {status_code}: {url}")
                        continue
                    
                    # Kiểm tra dữ liệu có hợp lệ không
                    if not is_valid_job_data(job_data):
                        invalid_data_links.append({"url": url, "timestamp": datetime.now().isoformat()})
                        total_invalid += 1
                        print(f"Invalid data (all null): {url}")
                        continue
                    
                    # Check trùng trước khi insert
                    if not collection.find_one({"post_link": job_data["post_link"]}):
                        collection.insert_one(job_data)
                        total_inserted += 1
                        print(f"Inserted: {job_data.get('job_name', 'N/A')[:50]} - {url}")
                    else:
                        total_skipped += 1
                        print(f"⏭Duplicate skipped: {url}")
                        
                except Exception as e:
                    print(f"Exception during processing {url}: {e}")

        # Lưu các link lỗi sau mỗi batch để tránh mất dữ liệu
        if error_404_links:
            save_error_links(error_404_links, "data/errors/404_errors.csv")
            error_404_links = []
        
        if error_429_links:
            save_error_links(error_429_links, "data/errors/429_rate_limit.csv")
            error_429_links = []
        
        if invalid_data_links:
            save_error_links(invalid_data_links, "data/errors/invalid_data.csv")
            invalid_data_links = []

        # Nghỉ giữa các batch để tránh bị block
        sleep_time = random.uniform(5, 15)
        print(f"\n==== Finished batch {batch_num}, sleep {sleep_time:.1f}s ====")
        time.sleep(sleep_time)