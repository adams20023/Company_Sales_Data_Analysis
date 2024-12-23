-- create_tables.sql
CREATE TABLE sales (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(50),
    sale_amount DECIMAL,
    sale_date DATE
);

CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    position VARCHAR(50),
    hire_date DATE
);
