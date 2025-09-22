-- Create a Products table
CREATE TABLE Products (
    ProductID INT PRIMARY KEY AUTO_INCREMENT,
    Name VARCHAR(100) NOT NULL,
    Price DECIMAL(10, 2) NOT NULL,
    CategoryID INT
);

-- Insert sample product data
INSERT INTO Products (Name, Price, CategoryID)
VALUES ('Laptop', 999.99, 1), ('Smartphone', 599.99, 1);

-- Query to retrieve products by category
SELECT Name, Price FROM Products WHERE CategoryID = 1;


