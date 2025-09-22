# Real-World Coding Environment Examples for Module 4: Integrating SQL with Python (MySQL)

# These examples demonstrate integrating SQL with Python using pymysql, SQLAlchemy, and pandas for MySQL.
# Each section includes multiple Python examples for connecting, querying, ETL, and hands-on exercises.
# Assume MySQL is running locally with a database 'company_db' (user: root, password: password).

import pymysql
import pandas as pd
from sqlalchemy import create_engine, text

# 1. Using Python Libraries to Connect to MySQL
# Scenario: A data analyst connects to a MySQL database to manage employee data.
# Example 1: Using pymysql for a MySQL connection.
conn_pymysql = pymysql.connect(
    host="localhost",
    user="root",
    password="password",
    database="company_db",
    port=3306
)
cursor = conn_pymysql.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS Employees (
    EmployeeID INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(100),
    Department VARCHAR(50),
    Salary DECIMAL(10, 2)
)
""")
conn_pymysql.commit()
# Insert sample data
cursor.execute("INSERT IGNORE INTO Employees (Name, Department, Salary) VALUES ('Alice', 'IT', 85000.00)")
conn_pymysql.commit()
# Query to verify
cursor.execute("SELECT * FROM Employees")
print(cursor.fetchall())
conn_pymysql.close()

# Example 2: Using SQLAlchemy for a MySQL connection.
engine = create_engine('mysql+pymysql://root:password@localhost:3306/company_db')
with engine.connect() as conn:
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS Departments (
        DepartmentID INT AUTO_INCREMENT PRIMARY KEY,
        DepartmentName VARCHAR(50)
    )
    """))
    conn.commit()
    # Insert data
    conn.execute(text("INSERT IGNORE INTO Departments (DepartmentName) VALUES ('IT')"))
    conn.commit()
    result = conn.execute(text("SELECT * FROM Departments"))
    print(result.fetchall())

# Example 3: Using pandas with SQLAlchemy for dataframe connections.
df = pd.read_sql("SELECT * FROM Employees", engine)
print(df)

# 2. Executing SQL Queries from Python Scripts and Handling Results as Dataframes
# Scenario: A developer automates sales report generation from a MySQL database.
# Example 1: Execute SELECT with pymysql and convert to dataframe.
conn_pymysql = pymysql.connect(
    host="localhost",
    user="root",
    password="password",
    database="company_db",
    port=3306
)
cursor = conn_pymysql.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS Sales (
    SaleID INT AUTO_INCREMENT PRIMARY KEY,
    Product VARCHAR(100),
    Amount DECIMAL(10, 2)
)
""")
cursor.execute("INSERT IGNORE INTO Sales (Product, Amount) VALUES ('Laptop', 999.99)")
conn_pymysql.commit()
# Fetch results
cursor.execute("SELECT * FROM Sales WHERE Amount > 500")
results = cursor.fetchall()
df_sales = pd.DataFrame(results, columns=['SaleID', 'Product', 'Amount'])
print(df_sales)
conn_pymysql.close()

# Example 2: Execute INSERT/UPDATE/DELETE with SQLAlchemy.
engine = create_engine('mysql+pymysql://root:password@localhost:3306/company_db')
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
# Scenario: An ETL process for cleaning and loading customer data into MySQL.
# Example 1: Simple ETL with pymysql and pandas.
conn_pymysql = pymysql.connect(
    host="localhost",
    user="root",
    password="password",
    database="company_db",
    port=3306
)
df_extract = pd.read_sql("SELECT * FROM Employees", conn_pymysql)
# Transform: Increase salary by 10%, handle missing
df_extract['Salary'] = df_extract['Salary'] * 1.10
df_extract = df_extract.dropna()
# Load back
df_extract.to_sql('Employees_Updated', conn_pymysql, if_exists='replace', index=False)
conn_pymysql.close()
print(df_extract)

# Example 2: ETL with SQLAlchemy for transformation.
engine = create_engine('mysql+pymysql://root:password@localhost:3306/company_db')
df_extract = pd.read_sql("SELECT * FROM Sales", engine)
# Transform: Aggregate and filter
df_transform = df_extract.groupby('Product').agg({'Amount': 'sum'}).reset_index()
df_transform = df_transform[df_transform['Amount'] > 1000]
# Load
df_transform.to_sql('Sales_Summary', engine, if_exists='replace', index=False)
print(df_transform)

# Example 3: Full ETL pipeline script.
def etl_pipeline():
    engine = create_engine('mysql+pymysql://root:password@localhost:3306/company_db')
    df = pd.read_sql("SELECT * FROM Employees", engine)
    # Transform: Add bonus, clean duplicates
    df['Bonus'] = df['Salary'] * 0.05
    df = df.drop_duplicates(subset=['Name'])
    # Load
    df.to_sql('Employees_With_Bonus', engine, if_exists='replace', index=False)
    return df

print(etl_pipeline())

# 4. Hands-On Exercises: Scripting SQL Operations in Python and Basic Data Cleaning
# Scenario: A script for managing inventory data with cleaning in MySQL.
# Example 1: Script with pymysql for operations and cleaning.
conn_pymysql = pymysql.connect(
    host="localhost",
    user="root",
    password="password",
    database="company_db",
    port=3306
)
cursor = conn_pymysql.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS Inventory (
    ItemID INT AUTO_INCREMENT PRIMARY KEY,
    ItemName VARCHAR(100),
    Quantity INT
)
""")
cursor.execute("INSERT IGNORE INTO Inventory (ItemName, Quantity) VALUES ('Laptop', 10)")
cursor.execute("INSERT IGNORE INTO Inventory (ItemName, Quantity) VALUES ('Mouse', NULL)")
conn_pymysql.commit()
# Extract and clean
df_inventory = pd.read_sql("SELECT * FROM Inventory", conn_pymysql)
df_inventory['Quantity'] = df_inventory['Quantity'].fillna(0)
df_inventory.to_sql('Inventory_Cleaned', conn_pymysql, if_exists='replace', index=False)
print(df_inventory)
conn_pymysql.close()

# Example 2: SQLAlchemy script for updates and cleaning.
engine = create_engine('mysql+pymysql://root:password@localhost:3306/company_db')
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