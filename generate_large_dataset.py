import pandas as pd
from sqlalchemy import create_engine
from faker import Faker
import random
import datetime

# Initialize Faker
fake = Faker()

# Database connection
engine = create_engine('postgresql://localhost/company_sales')

# Generate large dataset for sales
sales_data = []
for _ in range(10000):  # Adjust the range for larger datasets
    sales_data.append({
        'product_name': fake.word(),
        'sale_amount': round(random.uniform(10.0, 500.0), 2),
        'sale_date': fake.date_between(start_date='-1y', end_date='today')
    })
sales_df = pd.DataFrame(sales_data)

# Generate large dataset for employees
employees_data = []
for _ in range(1000):  # Adjust the range for larger datasets
    employees_data.append({
        'name': fake.name(),
        'position': fake.job(),
        'hire_date': fake.date_between(start_date='-10y', end_date='today')
    })
employees_df = pd.DataFrame(employees_data)

# Insert data into PostgreSQL
sales_df.to_sql('sales', engine, if_exists='append', index=False)
employees_df.to_sql('employees', engine, if_exists='append', index=False)

print("Large dataset generated and inserted into PostgreSQL")
