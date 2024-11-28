from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['your_email@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'pipeline1_scraper',
    default_args=default_args,
    description='Scrape articles and perform sentiment analysis',
    schedule_interval='0 19 * * 1-5',
    start_date=datetime(2024, 11, 27),
    catchup=False,
)

# Task 1: Scrape Data
def fetch_articles():
    sources = {
        "YourStory": "https://yourstory.com",
        "Finshots": "https://finshots.in/",
    }
    keywords = ["HDFC", "Tata Motors"]
    articles = []
    for source, url in sources.items():
        # Mock fetching articles
        articles.append({
            "source": source,
            "ticker": "HDFC",
            "title": f"Sample article for HDFC from {source}",
            "text": f"Sample text content from {source}",
        })
    pd.DataFrame(articles).to_csv('/tmp/articles.csv', index=False)

# Task 2: Clean and Process Data
def clean_data():
    df = pd.read_csv('/tmp/articles.csv')
    df = df.drop_duplicates(subset=['title'])
    df.to_csv('/tmp/cleaned_articles.csv', index=False)

# Task 3: Sentiment Analysis
def sentiment_analysis():
    df = pd.read_csv('/tmp/cleaned_articles.csv')
    df['sentiment'] = df['text'].apply(lambda x: round(requests.get("http://mock.sentiment.api", params={"text": x}).json()["score"], 2))
    df.to_csv('/tmp/sentiment_results.csv', index=False)

# Task 4: Persist Data
def persist_data():
    df = pd.read_csv('/tmp/sentiment_results.csv')
    # Mock DB persistence
    print("Data persisted:", df)

fetch_task = PythonOperator(task_id='fetch_articles', python_callable=fetch_articles, dag=dag)
clean_task = PythonOperator(task_id='clean_data', python_callable=clean_data, dag=dag)
sentiment_task = PythonOperator(task_id='sentiment_analysis', python_callable=sentiment_analysis, dag=dag)
persist_task = PythonOperator(task_id='persist_data', python_callable=persist_data, dag=dag)

fetch_task >> clean_task >> sentiment_task >> persist_task
