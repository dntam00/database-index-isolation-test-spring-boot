CREATE
    DATABASE IF NOT EXISTS index_practice;

-- Create user with password
CREATE
    USER IF NOT EXISTS 'app_user'@'%' IDENTIFIED BY 'password';

-- Grant privileges to the user for the database
GRANT ALL PRIVILEGES ON index_practice.* TO
    'app_user'@'%';

-- Apply the privileges
FLUSH
    PRIVILEGES;

-- Use the created database
USE
    index_practice;

-- Optional: Create a test table
CREATE TABLE IF NOT EXISTS person
(
    id
        BIGINT
        AUTO_INCREMENT
        PRIMARY KEY,
    name
        VARCHAR(255) NOT NULL
);