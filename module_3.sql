-- Real-World Coding Environment Examples for Module 3: Advanced SQL Techniques

-- 1. Joins (INNER, LEFT, RIGHT, FULL) and Subqueries for Complex Data Retrieval
-- Scenario: An e-commerce platform retrieves customer order details with product information.
-- Coding Environment: MySQL with a Node.js application to display order summaries.
-- Application: Use INNER and LEFT JOINs to combine customer, order, and product data; subqueries to filter high-value orders.
-- Sample Code (MySQL):
CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY AUTO_INCREMENT,
    Name VARCHAR(100)
);

CREATE TABLE Orders (
    OrderID INT PRIMARY KEY AUTO_INCREMENT,
    CustomerID INT,
    OrderDate DATE,
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);

CREATE TABLE OrderDetails (
    OrderDetailID INT PRIMARY KEY AUTO_INCREMENT,
    OrderID INT,
    ProductID INT,
    Quantity INT,
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID)
);

CREATE TABLE Products (
    ProductID INT PRIMARY KEY AUTO_INCREMENT,
    ProductName VARCHAR(100),
    Price DECIMAL(10, 2)
);

-- Insert sample data
INSERT INTO Customers (Name) VALUES ('Alice'), ('Bob');
INSERT INTO Orders (CustomerID, OrderDate) VALUES (1, '2025-09-01'), (2, '2025-09-02');
INSERT INTO Products (ProductName, Price) VALUES ('Laptop', 999.99), ('Mouse', 29.99);
INSERT INTO OrderDetails (OrderID, ProductID, Quantity) VALUES (1, 1, 2), (1, 2, 1);

-- INNER JOIN to retrieve order details
SELECT c.Name, o.OrderID, p.ProductName, od.Quantity
FROM Customers c
INNER JOIN Orders o ON c.CustomerID = o.CustomerID
INNER JOIN OrderDetails od ON o.OrderID = od.OrderID
INNER JOIN Products p ON od.ProductID = p.ProductID;

-- LEFT JOIN to include customers without orders
SELECT c.Name, COUNT(o.OrderID) AS OrderCount
FROM Customers c
LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.Name;

-- Subquery to find high-value orders
SELECT OrderID, CustomerID
FROM Orders
WHERE OrderID IN (
    SELECT OrderID
    FROM OrderDetails od
    JOIN Products p ON od.ProductID = p.ProductID
    WHERE p.Price * od.Quantity > 1000
);

-- 2. Aggregate Functions (SUM, AVG, COUNT, GROUP BY, HAVING) and Window Functions
-- Scenario: A company analyzes employee salaries by department to identify top earners.
-- Coding Environment: PostgreSQL with a Python application (using psycopg2) for reporting.
-- Application: Use aggregates to summarize salaries and window functions to rank employees.
-- Sample Code (PostgreSQL):
CREATE TABLE Departments (
    DepartmentID SERIAL PRIMARY KEY,
    DepartmentName VARCHAR(50)
);

CREATE TABLE Employees (
    EmployeeID SERIAL PRIMARY KEY,
    Name VARCHAR(100),
    DepartmentID INT,
    Salary DECIMAL(10, 2),
    FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID)
);

-- Insert sample data
INSERT INTO Departments (DepartmentName) VALUES ('IT'), ('HR');
INSERT INTO Employees (Name, DepartmentID, Salary)
VALUES ('Alice', 1, 85000.00), ('Bob', 1, 90000.00), ('Carol', 2, 60000.00);

-- Aggregate: Calculate average salary by department
SELECT d.DepartmentName, AVG(e.Salary) AS AvgSalary
FROM Employees e
JOIN Departments d ON e.DepartmentID = d.DepartmentID
GROUP BY d.DepartmentName
HAVING AVG(e.Salary) > 70000;

-- Window Function: Rank employees by salary within department
SELECT e.Name, e.Salary, d.DepartmentName,
       RANK() OVER (PARTITION BY e.DepartmentID ORDER BY e.Salary DESC) AS SalaryRank
FROM Employees e
JOIN Departments d ON e.DepartmentID = d.DepartmentID;

-- 3. Indexing, Views, and Stored Procedures for Performance Optimization
-- Scenario: A logistics company optimizes a database for tracking shipments.
-- Coding Environment: SQL Server with a .NET application for real-time tracking.
-- Application: Create indexes for frequent queries, views for simplified reporting, and stored procedures for repetitive tasks.
-- Sample Code (SQL Server):
CREATE TABLE Shipments (
    ShipmentID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT,
    ShipmentDate DATETIME,
    Status NVARCHAR(50)
);

-- Create index for frequent queries on ShipmentDate
CREATE INDEX IX_Shipments_ShipmentDate ON Shipments(ShipmentDate);

-- Create view for active shipments
CREATE VIEW ActiveShipments AS
SELECT ShipmentID, CustomerID, ShipmentDate
FROM Shipments
WHERE Status = 'In Transit';

-- Create stored procedure to update shipment status
CREATE PROCEDURE UpdateShipmentStatus
    @ShipmentID INT,
    @NewStatus NVARCHAR(50)
AS
BEGIN
    UPDATE Shipments
    SET Status = @NewStatus
    WHERE ShipmentID = @ShipmentID;
END;

-- Insert sample data
INSERT INTO Shipments (CustomerID, ShipmentDate, Status)
VALUES (101, '2025-09-01 10:00:00', 'In Transit'), (102, '2025-09-02 12:00:00', 'Delivered');

-- Query view for active shipments
SELECT * FROM ActiveShipments;

-- Execute stored procedure
EXEC UpdateShipmentStatus @ShipmentID = 1, @NewStatus = 'Delivered';

-- Analyze query performance (example using EXPLAIN)
-- Note: In SQL Server, use SET SHOWPLAN_ALL ON or Query Execution Plan in SSMS
SET SHOWPLAN_ALL ON;
SELECT * FROM Shipments WHERE ShipmentDate > '2025-09-01';
SET SHOWPLAN_ALL OFF;

-- 4. Hands-On Exercises: Building Multi-Table Queries and Analyzing Query Performance
-- Scenario: A retail chain analyzes sales data across stores and products.
-- Coding Environment: MySQL with MySQL Workbench to build and optimize queries.
-- Application: Write multi-table queries and analyze performance with EXPLAIN.
-- Sample Code (MySQL):
CREATE TABLE Stores (
    StoreID INT PRIMARY KEY AUTO_INCREMENT,
    StoreName VARCHAR(100)
);

CREATE TABLE Products (
    ProductID INT PRIMARY KEY AUTO_INCREMENT,
    ProductName VARCHAR(100),
    Price DECIMAL(10, 2)
);

CREATE TABLE Sales (
    SaleID INT PRIMARY KEY AUTO_INCREMENT,
    StoreID INT,
    ProductID INT,
    Quantity INT,
    SaleDate DATE,
    FOREIGN KEY (StoreID) REFERENCES Stores(StoreID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);

-- Insert sample data
INSERT INTO Stores (StoreName) VALUES ('Downtown'), ('Suburb');
INSERT INTO Products (ProductName, Price) VALUES ('Laptop', 999.99), ('Mouse', 29.99);
INSERT INTO Sales (StoreID, ProductID, Quantity, SaleDate)
VALUES (1, 1, 2, '2025-09-01'), (2, 2, 5, '2025-09-02');

-- Multi-table query: Total sales by store and product
SELECT s.StoreName, p.ProductName, SUM(sal.Quantity * p.Price) AS TotalRevenue
FROM Sales sal
INNER JOIN Stores s ON sal.StoreID = s.StoreID
INNER JOIN Products p ON sal.ProductID = p.ProductID
GROUP BY s.StoreName, p.ProductName
HAVING SUM(sal.Quantity * p.Price) > 100;

-- Create index for performance
CREATE INDEX IX_Sales_SaleDate ON Sales(SaleDate);

-- Analyze query performance
EXPLAIN
SELECT s.StoreName, COUNT(sal.SaleID) AS SaleCount
FROM Sales sal
JOIN Stores s ON sal.StoreID = s.StoreID
WHERE sal.SaleDate > '2025-09-01'
GROUP BY s.StoreName;