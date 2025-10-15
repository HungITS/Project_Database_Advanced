import pandas as pd
from datetime import datetime
import os
import re
from config.db_connection import connect
import sys

def clean_text(text):
    """Làm sạch text, loại bỏ ký tự đặc biệt"""
    if not text or not isinstance(text, str):
        return text
    text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def array_to_text(arr, separator='; '):
    """Convert array thành text với separator"""
    if not arr or not isinstance(arr, list):
        return None
    cleaned = [clean_text(str(item)) for item in arr if item]
    return separator.join(cleaned) if cleaned else None

def process_job_document(job):
    """Làm phẳng và xử lý từng document"""
    processed = {
        'company_name': job.get('company_name'),
        'company_link': job.get('company_links'),
        'job_name': job.get('job_name'),
        'post_link': job.get('post_link'),
        'application_deadline': job.get('application_deadline'),
    }

    # Mảng thông tin
    processed['tags'] = array_to_text(job.get('tags', []), ', ')
    processed['tags_count'] = len(job.get('tags', []))

    processed['job_detail'] = array_to_text(job.get('job_detail', []), '\n')
    processed['job_detail_points'] = len(job.get('job_detail', []))

    processed['candidate_requirements'] = array_to_text(job.get('candidate_requirements', []), '\n')
    processed['requirements_count'] = len(job.get('candidate_requirements', []))

    processed['benefits'] = array_to_text(job.get('interest', []), '\n')
    processed['benefits_count'] = len(job.get('interest', []))

    processed['location'] = array_to_text(job.get('location', []), ', ')
    processed['working_time'] = array_to_text(job.get('working_time', []), ', ')

    # Nested object
    info = job.get('general_info', {}) or {}
    processed['level'] = info.get('level')
    processed['education'] = info.get('education')
    processed['quantity'] = info.get('quantity')
    processed['form_of_work'] = info.get('form_of_work')

    return processed

if __name__ == "__main__":

    db, collection = connect()
    if db is None or collection is None:
        print("Cannot connect to MongoDB")
        exit(1)

    # Fetch Data
    print("\nFetching data from MongoDB...")
    jobs = list(collection.find({}))
    total_jobs = len(jobs)
    print(f"Found {total_jobs} documents")
    if total_jobs == 0:
        print("No data found, exiting.")
        sys.exit(0)

    # Clean + Flatten
    print("\nProcessing and cleaning data...")
    processed_jobs = []
    for i, job in enumerate(jobs, 1):
        try:
            processed_jobs.append(process_job_document(job))
        except Exception as e:
            print(f"Error at record {i}: {e}")
        if i % 200 == 0:
            print(f"   Processed {i}/{total_jobs}...")

    df = pd.DataFrame(processed_jobs)

    # Reorder Columns
    columns = [
        'job_name','company_name','level','education','form_of_work',
        'location','quantity','application_deadline','tags','tags_count',
        'job_detail','job_detail_points','candidate_requirements','requirements_count',
        'benefits','benefits_count','working_time','company_link','post_link'
    ]
    df = df[[c for c in columns if c in df.columns]]
    df.columns = [c.replace('_',' ').title() for c in df.columns]

    # Export Folder
    output_dir = "data/processed"
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    main_file = f"{output_dir}/jobs_clean_{timestamp}.csv"
    latest_file = f"{output_dir}/jobs_clean_latest.csv"

    df.to_csv(main_file, index=False, encoding='utf-8-sig')
    df.to_csv(latest_file, index=False, encoding='utf-8-sig')
    print(f"\nExported main file: {main_file}")

    # Create Summary Files
    print("\nGenerating analysis files...")

    # Company summary
    if 'Company Name' in df.columns:
        company_df = df.groupby('Company Name').agg({
            'Job Name':'count','Post Link':'first','Company Link':'first'
        }).reset_index().rename(columns={'Job Name':'Total Jobs'})
        company_df.sort_values('Total Jobs',ascending=False).to_csv(f"{output_dir}/company_summary.csv",index=False,encoding='utf-8-sig')
        print("Done company_summary.csv")

    # Tags breakdown
    if 'Tags' in df.columns:
        tags_data = []
        for _, row in df.iterrows():
            if pd.notna(row['Tags']):
                for tag in [t.strip() for t in str(row['Tags']).split(',') if t.strip()]:
                    tags_data.append({
                        'Job Name': row['Job Name'],
                        'Company Name': row['Company Name'],
                        'Tag': tag,
                        'Level': row.get('Level'),
                        'Location': row.get('Location')
                    })
        if tags_data:
            tags_df = pd.DataFrame(tags_data)
            tags_df.to_csv(f"{output_dir}/tags_breakdown.csv", index=False, encoding='utf-8-sig')
            tags_df['Tag'].value_counts().reset_index().rename(columns={'index':'Tag','Tag':'Frequency'}) \
                .to_csv(f"{output_dir}/tag_frequency.csv", index=False, encoding='utf-8-sig')
            print("Done tags_breakdown.csv + tag_frequency.csv")

    # Level analysis
    if 'Level' in df.columns:
        lvl = df.groupby('Level').agg({'Job Name':'count','Company Name':'nunique','Tags Count':'mean'}) \
                .reset_index().rename(columns={'Job Name':'Total Jobs','Company Name':'Unique Companies','Tags Count':'Avg Tags'})
        lvl.to_csv(f"{output_dir}/level_analysis.csv", index=False, encoding='utf-8-sig')
        print("Done level_analysis.csv")

    # Location breakdown
    if 'Location' in df.columns:
        loc_rows = []
        for _, row in df.iterrows():
            if pd.notna(row['Location']):
                for loc in [l.strip() for l in str(row['Location']).split(';') if l.strip()]:
                    loc_rows.append({
                        'Job Name': row['Job Name'],
                        'Company Name': row['Company Name'],
                        'Location': loc,
                        'Level': row.get('Level')
                    })
        if loc_rows:
            loc_df = pd.DataFrame(loc_rows)
            loc_df.to_csv(f"{output_dir}/location_breakdown.csv", index=False, encoding='utf-8-sig')
            loc_df['Location'].value_counts().reset_index().rename(columns={'index':'Location','Location':'Jobs Count'}) \
                .to_csv(f"{output_dir}/location_frequency.csv", index=False, encoding='utf-8-sig')
            print("Done location_breakdown.csv + location_frequency.csv")
