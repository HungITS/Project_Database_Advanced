import numpy as np
import time
import pandas as pd
from bs4 import BeautifulSoup
import cloudscraper
from concurrent.futures import ThreadPoolExecutor
import random
import os

# Tạo scraper
scraper = cloudscraper.create_scraper()

def get_links(page):
    time.sleep(random.uniform(1,3))
    page= str(page+1)
    headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
    }

    link ="https://www.topcv.vn/viec-lam-it?sort=&skill_id=&skill_id_other=&keyword=&position=&salary=&lc=1&ld=&location_query_string=l1&page="+page

    # Gửi yêu cầu HTTP bằng requests
    #r = requests.get(link, headers=headers)
    try: 
        response = scraper.get(link, headers=headers)
        if response.status_code == 200:
        # Phân tích nội dung HTML bằng BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')
        
        # Tìm tất cả các thẻ <a> có class 'crd7gu7'
            a = soup.find_all("a", target="_blank")

        # Lấy href từ các thẻ <a> và thêm vào danh sách
            links = []
            for i in range(len(a)):
                link = a[i]['href']
                links.append(link)
            
            return links
        else:
            print(f"Failed to fetch page {page}, status code: {response.status_code}")
            return []
    except Exception as error:
        print(error)
        return []


if __name__ == "__main__":

    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(get_links, range(29)))

    arr = [link for sublist in results for link in sublist]

    arr = list(set(arr))

    # Tạo DataFrame từ danh sách các link
    df_links = pd.DataFrame(arr, columns=['links'])

    # Đảm bảo thư mục data/raw tồn tại
    output_dir = os.path.join("data", "raw")
    os.makedirs(output_dir, exist_ok=True)

    # Đường dẫn file CSV
    output_path = os.path.join(output_dir, "links.csv")

    # Lưu file CSV
    df_links.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"File saved to: {output_path}")