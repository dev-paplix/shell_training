# Real-World Coding Environment Examples for Module 4: Integrating SQL with Python

# These examples demonstrate integrating SQL with Python using sqlite3, SQLAlchemy, and pandas.
# Each section includes multiple Python examples for connecting, querying, ETL, and hands-on exercises.
# Assume a SQLite database for simplicity, but concepts apply to other DBMS.

import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text

# 1. Using Python Libraries like sqlite3, SQLAlchemy, or pandas to Connect to Databases
# Scenario: A data analyst connects to a company database to access employee data.
# Example 1: Using sqlite3 for a simple file-based connection.
conn_sqlite = sqlite3.connect('company.db')
cursor = conn_sqlite.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS Employees (
    EmployeeID INTEGER PRIMARY KEY,
    Name TEXT,
    Department TEXT,
    Salary REAL
)''')
conn_sqlite.commit()
# Insert sample data
cursor.execute("INSERT OR IGNORE INTO Employees VALUES (1, 'Alice', 'IT', 85000.00)")
conn_sqlite.commit()
# Query to verify
cursor.execute("SELECT * FROM Employees")
print(cursor.fetchall())
conn_sqlite.close()

# Example 2: Using SQLAlchemy for a more flexible connection (e.g., to SQLite or other DBMS).
engine = create_engine('sqlite:///company.db')
with engine.connect() as conn:
    conn.execute(text('''CREATE TABLE IF NOT EXISTS Departments (
        DepartmentID INTEGER PRIMARY KEY,
        DepartmentName TEXT
    )'''))
    conn.commit()
    # Insert data
    conn.execute(text("INSERT OR IGNORE INTO Departments VALUES (1, 'IT')"))
    conn.commit()
    result = conn.execute(text("SELECT * FROM Departments"))
    print(result.fetchall())

# Example 3: Using pandas with SQLAlchemy for direct dataframe connections.
df = pd.read_sql("SELECT * FROM Employees", engine)
print(df)

# 2. Executing SQL Queries from Python Scripts and Handling Results as Dataframes
# Scenario: A developer automates report generation by querying sales data.
# Example 1: Execute SELECT with sqlite3 and convert to dataframe.
conn_sqlite = sqlite3.connect('company.db')
cursor = conn_sqlite.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS Sales (
    SaleID INTEGER PRIMARY KEY,
    Product TEXT,
    Amount REAL
)''')
cursor.execute("INSERT OR IGNORE INTO Sales VALUES (1, 'Laptop', 999.99)")
conn_sqlite.commit()
# Fetch results
cursor.execute("SELECT * FROM Sales WHERE Amount > 500")
results = cursor.fetchall()
df_sales = pd.DataFrame(results, columns=['SaleID', 'Product', 'Amount'])
print(df_sales)
conn_sqlite.close()

# Example 2: Execute INSERT/UPDATE/DELETE with SQLAlchemy.
engine = create_engine('sqlite:///company.db')
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
JOIN Sales s ON e.EmployeeID = s.SaleID  -- Assuming a relationship for example
""", engine)
print(df_joined)

# 3. Data Extraction, Transformation, and Loading (ETL) Basics with Python
# Scenario: An ETL process for cleaning and loading customer data into a database.
# Example 1: Simple ETL with sqlite3 and pandas.
# Extract
conn_sqlite = sqlite3.connect('company.db')
df_extract = pd.read_sql("SELECT * FROM Employees", conn_sqlite)
# Transform: Clean data (e.g., increase salary by 10%, handle missing)
df_extract['Salary'] = df_extract['Salary'] * 1.10
df_extract = df_extract.dropna()
# Load back
df_extract.to_sql('Employees_Updated', conn_sqlite, if_exists='replace', index=False)
conn_sqlite.close()
print(df_extract)

# Example 2: ETL with SQLAlchemy for transformation.
engine = create_engine('sqlite:///company.db')
df_extract = pd.read_sql("SELECT * FROM Sales", engine)
# Transform: Aggregate and filter
df_transform = df_extract.groupby('Product').agg({'Amount': 'sum'}).reset_index()
df_transform = df_transform[df_transform['Amount'] > 1000]
# Load
df_transform.to_sql('Sales_Summary', engine, if_exists='replace', index=False)
print(df_transform)

# Example 3: Full ETL pipeline script.
# Extract from one table, transform, load to another.
def etl_pipeline():
    engine = create_engine('sqlite:///company.db')
    df = pd.read_sql("SELECT * FROM Employees", engine)
    # Transform: Add a new column, clean duplicates
    df['Bonus'] = df['Salary'] * 0.05
    df = df.drop_duplicates(subset=['Name'])
    # Load
    df.to_sql('Employees_With_Bonus', engine, if_exists='replace', index=False)
    return df

print(etl_pipeline())

# 4. Hands-On Exercises: Scripting SQL Operations in Python and Basic Data Cleaning
# Scenario: A script for managing inventory data with cleaning.
# Example 1: Script with sqlite3 for operations and cleaning.
conn_sqlite = sqlite3.connect('company.db')
cursor = conn_sqlite.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS Inventory (
    ItemID INTEGER PRIMARY KEY,
    ItemName TEXT,
    Quantity INTEGER
)''')
cursor.execute("INSERT OR IGNORE INTO Inventory VALUES (1, 'Laptop', 10)")
cursor.execute("INSERT OR IGNORE INTO Inventory VALUES (2, 'Mouse', NULL)")  # Missing value
conn_sqlite.commit()
# Extract and clean
df_inventory = pd.read_sql("SELECT * FROM Inventory", conn_sqlite)
df_inventory['Quantity'] = df_inventory['Quantity'].fillna(0)  # Clean missing
df_inventory.to_sql('Inventory_Cleaned', conn_sqlite, if_exists='replace', index=False)
print(df_inventory)
conn_sqlite.close()

# Example 2: SQLAlchemy script for updates and cleaning.
engine = create_engine('sqlite:///company.db')
with engine.connect() as conn:
    conn.execute(text("UPDATE Inventory SET Quantity = 15 WHERE ItemName = 'Laptop'"))
    conn.commit()
df_clean = pd.read_sql("SELECT * FROM Inventory", engine)
# Clean: Remove duplicates, convert types
df_clean = df_clean.drop_duplicates()
df_clean['Quantity'] = df_clean['Quantity'].astype(int)
print(df_clean)

# Example 3: Hands-on ETL with data cleaning.
# Script to extract sales, clean outliers, load summary.
df_sales = pd.read_sql("SELECT * FROM Sales", engine)
# Clean: Remove outliers (e.g., Amount > 2000)
df_sales = df_sales[df_sales['Amount'] <= 2000]
# Transform: Add category
df_sales['Category'] = 'Electronics'
# Load
df_sales.to_sql('Sales_Cleaned', engine, if_exists='replace', index=False)
print(df_sales)