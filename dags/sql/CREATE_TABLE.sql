CREATE TABLE IF NOT EXISTS trades (
    trade_id INT NOT NULL PRIMARY KEY,
    trade_date DATE,
    trade_name VARCHAR(50),
    trade_type VARCHAR(50),
    trade_result FLOAT
);