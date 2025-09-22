-- Real-World Coding Environment Examples for Module 2: SQL Basics: Querying and Data Manipulation

-- 1. Setting Up a SQL Environment and Connecting to a Database
-- Scenario: A developer sets up a MySQL environment for a small business to manage employee data.
-- Coding Environment: MySQL with Python (using pymysql) to connect and query the database.
-- Application: Install MySQL, create a database, and connect using a Python script.
-- Sample Code (Python + MySQL):
/* Python connection script (for reference, not executed in SQL environment)
import pymysql
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='password',
    database='business_db'
)
cursor = connection.cursor()
*/
-- SQL to create and verify database
CREATE DATABASE business_db;
USE business_db;

-- 2. Core SQL Commands: SELECT, FROM, WHERE, ORDER BY, LIMIT
-- Scenario: A retail company queries sales data to find top-selling products.
-- Coding Environment: PostgreSQL with pgAdmin for query execution.
-- Application: Retrieve and filter sales data to identify high-value orders.
-- Sample Code (PostgreSQL):
CREATE TABLE Sales (
    OrderID SERIAL PRIMARY KEY,
    ProductName VARCHAR(100),
    Amount DECIMAL(10, 2),
    OrderDate DATE
);

INSERT INTO Sales (ProductName, Amount, OrderDate)
VALUES ('Laptop', 999.99, '2025-09-01'), ('Mouse', 29.99, '2025-09-02');

-- Query top 5 sales by amount
SELECT ProductName, Amount
FROM Sales
WHERE Amount > 50.00
ORDER BY Amount DESC
LIMIT 5;

-- 3. Data Insertion, Updates, and Deletion
-- Scenario: A human resources system manages employee records in SQL Server.
-- Coding Environment: SQL Server with a .NET application for data manipulation.
-- Application: Add, update, and delete employee records to maintain accurate data.
-- Sample Code (SQL Server):
CREATE TABLE Employees (
    EmployeeID INT IDENTITY(1,1) PRIMARY KEY,
    Name NVARCHAR(100),
    Department NVARCHAR(50),
    Salary DECIMAL(10, 2)
);

-- Insert new employee
INSERT INTO Employees (Name, Department, Salary)
VALUES ('Jane Doe', 'IT', 75000.00);

-- Update employee salary
UPDATE Employees
SET Salary = 80000.00
WHERE EmployeeID = 1;

-- Delete employee record
DELETE FROM Employees
WHERE EmployeeID = 1;

-- Query to verify changes
SELECT EmployeeID, Name, Salary
FROM Employees
WHERE Department = 'IT';

-- 4. Hands-On Exercises: Simple Queries on Sample Datasets
-- Scenario: A developer works with an employee dataset to generate reports for a company.
-- Coding Environment: MySQL with MySQL Workbench to query employee data.
-- Application: Query employee data to find high earners and recent hires.
-- Sample Code (MySQL):
CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY AUTO_INCREMENT,
    Name VARCHAR(100),
    Department VARCHAR(50),
    Salary DECIMAL(10, 2),
    HireDate DATE
);

-- Insert sample employee data
INSERT INTO Employees (Name, Department, Salary, HireDate)
VALUES 
    ('Alice Brown', 'Marketing', 65000.00, '2024-01-15'),
    ('Bob Smith', 'IT', 85000.00, '2025-03-10');

-- Query top earners
SELECT Name, Salary
FROM Employees
WHERE Salary > 70000.00
ORDER BY Salary DESC;

-- Query recent hires
SELECT Name, HireDate
FROM Employees
WHERE HireDate > '2025-01-01'
ORDER BY HireDate;

-- Update department for an employee
UPDATE Employees
SET Department = 'Sales'
WHERE EmployeeID = 1;

-- Delete employees with low salary
DELETE FROM Employees
WHERE Salary < 60000.00;

-- Verify final dataset
SELECT * FROM Employees;