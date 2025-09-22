-- Create Accounts table
CREATE TABLE Accounts (
    AccountID SERIAL PRIMARY KEY,
    CustomerID INT NOT NULL,
    Balance DECIMAL(15, 2) DEFAULT 0.00
);

-- Create Transactions table
CREATE TABLE Transactions (
    TransactionID SERIAL PRIMARY KEY,
    AccountID INT NOT NULL,
    Amount DECIMAL(15, 2),
    TransactionDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO Accounts (CustomerID, Balance) VALUES (1001, 5000.00);
INSERT INTO Transactions (AccountID, Amount) VALUES (1, -200.00);

-- Query account balance
SELECT Balance FROM Accounts WHERE AccountID = 1;