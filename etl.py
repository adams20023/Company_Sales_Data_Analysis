import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection
DATABASE_URL = os.getenv('DATABASE_URL')
engine = create_engine(DATABASE_URL)

def extract_data():
    try:
        sales_df = pd.read_sql('SELECT * FROM sales', engine)
        employees_df = pd.read_sql('SELECT * FROM employees', engine)
        logging.info("Data extraction completed")
        return sales_df, employees_df
    except SQLAlchemyError as e:
        logging.error("Error extracting data: %s", e)
        raise

def transform_data(sales_df, employees_df):
    try:
        # Example: Calculating total sales amount
        total_sales = sales_df['sale_amount'].sum()
        sales_df['total_sales'] = total_sales

        # Example: Data cleaning
        sales_df['product_name'] = sales_df['product_name'].str.strip()
        employees_df['name'] = employees_df['name'].str.strip()

        # Example: Adding a new column with derived data
        sales_df['sale_year'] = pd.to_datetime(sales_df['sale_date']).dt.year

        # Example: Aggregation
        sales_summary = sales_df.groupby('sale_year').agg(
            total_sales_amount=pd.NamedAgg(column='sale_amount', aggfunc='sum'),
            average_sale_amount=pd.NamedAgg(column='sale_amount', aggfunc='mean'),
            total_transactions=pd.NamedAgg(column='id', aggfunc='count')
        ).reset_index()

        logging.info("Data transformation completed")
        return sales_df, employees_df, sales_summary
    except Exception as e:
        logging.error("Error transforming data: %s", e)
        raise

def load_data(sales_df, employees_df, sales_summary):
    try:
        sales_df.to_csv('sales_data.csv', index=False)
        employees_df.to_csv('employees_data.csv', index=False)
        sales_summary.to_csv('sales_summary.csv', index=False)
        logging.info("Data loading completed")
    except Exception as e:
        logging.error("Error loading data: %s", e)
        raise

def etl_process():
    try:
        sales_df, employees_df = extract_data()
        transformed_sales_df, transformed_employees_df, sales_summary = transform_data(sales_df, employees_df)
        load_data(transformed_sales_df, transformed_employees_df, sales_summary)
        logging.info("ETL process completed successfully")
    except Exception as e:
        logging.error("ETL process failed: %s", e)

if __name__ == "__main__":
    etl_process()
