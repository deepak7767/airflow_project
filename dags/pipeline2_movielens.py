from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['your_email@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'pipeline2_movielens',
    default_args=default_args,
    description='Analyze MovieLens dataset',
    schedule_interval='0 20 * * 1-5',
    start_date=datetime(2024, 11, 27),
    catchup=False,
)

# Load Data
def load_data():
    data = pd.read_csv("http://files.grouplens.org/datasets/movielens/ml-100k/u.data", sep='\t', names=["user_id", "movie_id", "rating", "timestamp"])
    users = pd.read_csv("http://files.grouplens.org/datasets/movielens/ml-100k/u.user", sep='|', names=["user_id", "age", "gender", "occupation", "zip_code"])
    pd.merge(data, users, on="user_id").to_csv('/tmp/movielens.csv', index=False)

# Mean Age by Occupation
def mean_age_by_occupation():
    df = pd.read_csv('/tmp/movielens.csv')
    result = df.groupby('occupation')['age'].mean().reset_index()
    print(result)

# Top Rated Movies
def top_rated_movies():
    df = pd.read_csv('/tmp/movielens.csv')
    result = df.groupby('movie_id')['rating'].agg(['mean', 'count']).query('count >= 35').sort_values('mean', ascending=False).head(20)
    print(result)

# Top Genres by Age Group
def top_genres_by_age_group():
    df = pd.read_csv('/tmp/movielens.csv')
    df['age_group'] = pd.cut(df['age'], bins=[20, 25, 35, 45, 100], labels=['20-25', '25-35', '35-45', '45+'])
    result = df.groupby(['age_group', 'occupation'])['movie_id'].count().reset_index()
    print(result)

# Similar Movies
def similar_movies():
    # Add your similarity calculation here
    print("Calculate similarity matrix for
