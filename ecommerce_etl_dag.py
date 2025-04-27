from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import snowflake.connector

# Step 1: Extract data from API
def extract_data_from_api():
    url = "https://fakestoreapi.com/products"
    response = requests.get(url)
    data = response.json() 
    df = pd.DataFrame(data)
    df.to_csv('Products.CSV', index=False)
    print("Data Extracted & Saved!")

# Step 2: Transform data (cleaning, calculating new columns)
def transform_data():
    df = pd.read_csv('Products.CSV')
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    df['description'].fillna('No Description Available', inplace=True)

    # Clean string columns
    string_columns = df.select_dtypes(include=['object']).columns
    for col in string_columns:
        df[col] = df[col].str.strip()

    # Add calculated columns
    df['price_with_tax'] = (df['price'] * 1.15).round(2)        
    df['discounted_price'] = (df['price'] * 0.90).round(2)       
    df['profit_margin'] = (df['price_with_tax'] - df['discounted_price']).round(2)

    # Create price category
    def price_category(price):
        if price < 50:
            return 'Low'
        elif 50 <= price <= 150:
            return 'Medium'
        else:
            return 'High'

    df['price_category'] = df['price'].apply(price_category)

    # Save cleaned data
    df.to_csv('cleaned_products.csv', index=False)
    print("Data Transformed & Saved!")

# Step 3: Load data into Snowflake
def upload_to_snowflake():
    # Step 1: Read CSV
    df = pd.read_csv('cleaned_products.csv')

    # Step 2: Connect to Snowflake
    conn = snowflake.connector.connect(
        user='user_name',
        password='password',
        account='account_identifier',
        warehouse='',                     # like ==> COMPUTE_WH
        database='',
        schema=''
    )
    print("Connection Successful!")

    # Step 3: Insert Data
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO cleaned_products (
                id, title, price, description, category, image, rating,
                price_with_tax, discounted_price, profit_margin, price_category
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, tuple(row))

    print(f"{len(df)} rows uploaded successfully!")

    # Step 4: Close
    cur.close()
    conn.close()

# Airflow Default Arguments
default_args = {
    'owner': 'saad',
    'start_date': datetime(2025, 4, 28),
    'retries': 1,
}

# Airflow DAG Definition
with DAG('ecommerce_sales_etl',
         default_args=default_args,
         schedule_interval='@daily',  # Runs daily
         catchup=False) as dag:

    # Task 1: Extract data from API
    task1 = PythonOperator(
        task_id='extract_data_from_api_task',
        python_callable=extract_data_from_api
    )

    # Task 2: Transform data
    task2 = PythonOperator(
        task_id='transform_data_task',
        python_callable=transform_data
    )

    # Task 3: Upload data to Snowflake
    task3 = PythonOperator(
        task_id='upload_to_snowflake_task',
        python_callable=upload_to_snowflake
    )

    # Task dependencies (order of execution)
    task1 >> task2 >> task3
