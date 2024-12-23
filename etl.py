# etl.py
import pandas as pd
from sqlalchemy import create_engine

# Database connection
engine = create_engine('postgresql://localhost/company_sales')

try:
    # Extract data
    sales_df = pd.read_sql('SELECT * FROM sales', engine)
    employees_df = pd.read_sql('SELECT * FROM employees', engine)
    
    # Transform data - Example: Calculating total sales amount
    total_sales = sales_df['sale_amount'].sum()
    print(f"Total Sales Amount: {total_sales}")

    # Load data
    sales_df.to_csv('sales_data.csv', index=False)
    employees_df.to_csv('employees_data.csv', index=False)

    print("ETL process completed and data saved to CSV files.")

except Exception as e:
    print(f"An error occurred during the ETL process: {e}")
finally:
    # Cleanup (if needed, for example, closing the engine)
    engine.dispose()

