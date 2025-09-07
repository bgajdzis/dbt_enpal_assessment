-- Table for activity.csv
CREATE TABLE IF NOT EXISTS activity (
    activity_id INT,
    type VARCHAR(50),
    assigned_to_user INT,
    deal_id INT,
    done BOOLEAN,
    due_to TIMESTAMP
);

-- Table for deal_changes.csv
CREATE TABLE IF NOT EXISTS deal_changes (
    deal_id INT,
    change_time TIMESTAMP,
    changed_field_key VARCHAR(50),
    new_value VARCHAR(255)
);

-- Table for users.csv
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    modified TIMESTAMP
);

CREATE EXTENSION IF NOT EXISTS TABLEFUNC;
