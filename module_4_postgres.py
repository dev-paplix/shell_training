# Real-World Coding Environment Examples for Module 4: Integrating SQL with Python (PostgreSQL)

# These examples demonstrate integrating SQL with Python using psycopg2, SQLAlchemy, and pandas for PostgreSQL.
# Each section includes multiple Python examples for connecting, querying, ETL, and hands-on exercises.
# Assume PostgreSQL is running locally with a database 'company_db' (user: postgres, password: password).

import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text

# 1. Using Python Libraries to Connect to PostgreSQL
# Scenario: A data analyst connects to a PostgreSQL database to manage employee data.
# Example 1: Using psycopg2 for a PostgreSQL connection.
conn_psycopg = psycopg2.connect(
    dbname="company_db",
    user="postgres",
    password="password",
    host="localhost",
    port="5432"
)
cursor = conn_psycopg.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS Employees (
    EmployeeID SERIAL PRIMARY KEY,
    Name VARCHAR(100),
    Department VARCHAR(50),
    Salary NUMERIC(10, 2)
)
""")
conn_psycopg.commit()
# Insert sample data
cursor.execute("INSERT INTO Employees (Name, Department, Salary) VALUES ('Alice', 'IT', 85000.00) ON CONFLICT DO NOTHING")
conn_psycopg.commit()
# Query to verify
cursor.execute("SELECT * FROM Employees")
print(cursor.fetchall())
conn_psycopg.close()

# Example 2: Using SQLAlchemy for a PostgreSQL connection.
engine = create_engine('postgresql+psycopg2://postgres:password@localhost:5432/company_db')
with engine.connect() as conn:
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS Departments (
        DepartmentID SERIAL PRIMARY KEY,
        DepartmentName VARCHAR(50)
    )
    """))
    conn.commit()
    # Insert data
    conn.execute(text("INSERT INTO Departments (DepartmentName) VALUES ('IT') ON CONFLICT DO NOTHING"))
    conn.commit()
    result = conn.execute(text("SELECT * FROM Departments"))
    print(result.fetchall())

# Example 3: Using pandas with SQLAlchemy for dataframe connections.
df = pd.read_sql("SELECT * FROM Employees", engine)
print(df)

# 2. Executing SQL Queries from Python Scripts and Handling Results as Dataframes
# Scenario: A developer automates sales report generation from a PostgreSQL database.
# Example 1: Execute SELECT with psycopg2 and convert to dataframe.
conn_psycopg = psycopg2.connect(
    dbname="company_db",
    user="postgres",
    password="password",
    host="localhost",
    port="5432"
)
cursor = conn_psycopg.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS Sales (
    SaleID SERIAL PRIMARY KEY,
    Product VARCHAR(100),
    Amount NUMERIC(10, 2)
)
""")
cursor.execute("INSERT INTO Sales (Product, Amount) VALUES ('Laptop', 999.99) ON CONFLICT DO NOTHING")
conn_psycopg.commit()
# Fetch results
cursor.execute("SELECT * FROM Sales WHERE Amount > 500")
results = cursor.fetchall()
df_sales = pd.DataFrame(results, columns=['SaleID', 'Product', 'Amount'])
print(df_sales)
conn_psycopg.close()

# Example 2: Execute INSERT/UPDATE/DELETE with SQLAlchemy.
engine = create_engine('postgresql+psycopg2://postgres:password@localhost:5432/company_db')
with engine.connect() as conn:
    # INSERT
    conn.execute(text("INSERT INTO Sales (Product, Amount) VALUES ('Mouse', 29.99)"))
    # UPDATE
    conn.execute(text("UPDATE Sales SET Amount = 1099.99 WHERE Product = 'Laptop'"))
    # DELETE
    conn.execute(text("DELETE FROM Sales WHERE Amount < 50"))
    conn.commit()
    # SELECT as dataframe
    df_updated = pd.read_sql("SELECT * FROM Sales", conn)
    print(df_updated)

# Example 3: Complex query with pandas.
df_joined = pd.read_sql("""
SELECT e.Name, s.Product, s.Amount
FROM Employees e
LEFT JOIN Sales s ON e.EmployeeID = s.SaleID
""", engine)
print(df_joined)

# 3. Data Extraction, Transformation, and Loading (ETL) Basics with Python
# Scenario: An ETL process for cleaning and loading customer data into PostgreSQL.
# Example 1: Simple ETL with psycopg2 and pandas.
conn_psycopg = psycopg2.connect(
    dbname="company_db",
    user="postgres",
    password="password",
    host="localhost",
    port="5432"
)
df_extract = pd.read_sql("SELECT * FROM Employees", conn_psycopg)
# Transform: Increase salary by 10%, handle missing
df_extract['Salary'] = df_extract['Salary'] * 1.10
df_extract = df_extract.dropna()
# Load back
df_extract.to_sql('Employees_Updated', conn_psycopg, if_exists='replace', index=False)
conn_psycopg.close()
print(df_extract)

# Example 2: ETL with SQLAlchemy for transformation.
engine = create_engine('postgresql+psycopg2://postgres:password@localhost:5432/company_db')
df_extract = pd.read_sql("SELECT * FROM Sales", engine)
# Transform: Aggregate and filter
df_transform = df_extract.groupby('Product').agg({'Amount': 'sum'}).reset_index()
df_transform = df_transform[df_transform['Amount'] > 1000]
# Load
df_transform.to_sql('Sales_Summary', engine, if_exists='replace', index=False)
print(df_transform)

# Example 3: Full ETL pipeline script.
def etl_pipeline():
    engine = create_engine('postgresql+psycopg2://postgres:password@localhost:5432/company_db')
    df = pd.read_sql("SELECT * FROM Employees", engine)
    # Transform: Add bonus, clean duplicates
    df['Bonus'] = df['Salary'] * 0.05
    df = df.drop_duplicates(subset=['Name'])
    # Load
    df.to_sql('Employees_With_Bonus', engine, if_exists='replace', index=False)
    return df

print(etl_pipeline())

# 4. Hands-On Exercises: Scripting SQL Operations in Python and Basic Data Cleaning
# Scenario: A script for managing inventory data with cleaning in PostgreSQL.
# Example 1: Script with psycopg2 for operations and cleaning.
conn_psycopg = psycopg2.connect(
    dbname="company_db",
    user="postgres",
    password="password",
    host="localhost",
    port="5432"
)
cursor = conn_psycopg.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS Inventory (
    ItemID SERIAL PRIMARY KEY,
    ItemName VARCHAR(100),
    Quantity INTEGER
)
""")
cursor.execute("INSERT INTO Inventory (ItemName, Quantity) VALUES ('Laptop', 10) ON CONFLICT DO NOTHING")
cursor.execute("INSERT INTO Inventory (ItemName, Quantity) VALUES ('Mouse', NULL) ON CONFLICT DO NOTHING")
conn_psycopg.commit()
# Extract and clean
df_inventory = pd.read_sql("SELECT * FROM Inventory", conn_psycopg)
df_inventory['Quantity'] = df_inventory['Quantity'].fillna(0)
df_inventory.to_sql('Inventory_Cleaned', conn_psycopg, if_exists='replace', index=False)
print(df_inventory)
conn_psycopg.close()

# Example 2: SQLAlchemy script for updates and cleaning.
engine = create_engine('postgresql+psycopg2://postgres:password@localhost:5432/company_db')
with engine.connect() as conn:
    conn.execute(text("UPDATE Inventory SET Quantity = 15 WHERE ItemName = 'Laptop'"))
    conn.commit()
df_clean = pd.read_sql("SELECT * FROM Inventory", engine)
# Clean: Remove duplicates, convert types
df_clean = df_clean.drop_duplicates()
df_clean['Quantity'] = df_clean['Quantity'].astype(int)
print(df_clean)

# Example 3: Hands-on ETL with data cleaning.
df_sales = pd.read_sql("SELECT * FROM Sales", engine)
# Clean: Remove outliers (Amount > 2000)
df_sales = df_sales[df_sales['Amount'] <= 2000]
# Transform: Add category
df_sales['Category'] = 'Electronics'
# Load
df_sales.to_sql('Sales_Cleaned', engine, if_exists='replace', index=False)
print(df_sales)