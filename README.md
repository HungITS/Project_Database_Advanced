# Job ETL Pipeline - MongoDB & Power BI Dashboard

**End-to-end data pipeline** thu thập dữ liệu tin tuyển dụng tự động từ web, lưu trữ bằng MongoDB (NoSQL) và trực quan hóa bằng Power BI.

## 📋 Project Overview
Dự án xây dựng một pipeline hoàn chỉnh:
- **Crawling** dữ liệu tin tuyển dụng từ các trang web việc làm
- **Xử lý rate limit (429)** và retry thông minh
- **Lưu trữ** vào MongoDB (NoSQL)
- **Trực quan hóa** bằng Power BI để phân tích xu hướng thị trường lao động

## ✨ Features
- Tự động extract links và chi tiết tin tuyển dụng
- Xử lý lỗi 429 (Too Many Requests) với cơ chế retry
- Lưu dữ liệu cấu trúc vào MongoDB
- Export dữ liệu ra CSV
- Dashboard Power BI tương tác (phân tích vị trí, mức lương, kỹ năng, công ty...)

## 🛠️ Technologies Used
- **Python** – Web scraping & data processing
- **MongoDB** – NoSQL database (lưu trữ linh hoạt)
- **Power BI** – Interactive dashboard (file: `bi_analyst/Analysis.pbix`)
- **Requests / BeautifulSoup / Selenium** (tùy theo code)
- Rate-limit handling & retry logic

## 📊 Dashboard Highlights
- Phân tích số lượng việc làm theo ngành nghề / vị trí
- Mức lương trung bình theo kinh nghiệm
- Top công ty tuyển dụng
- Kỹ năng hot nhất trên thị trường
