CREATE DATABASE shifts_db;

\c shifts_db;

CREATE TABLE IF NOT EXISTS shift_requests (
    id UUID PRIMARY KEY,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    total_shifts INTEGER NOT NULL,
    processed INTEGER DEFAULT 0,
    successful INTEGER DEFAULT 0,
    failed INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS individual_shifts (
    id SERIAL PRIMARY KEY,
    request_id UUID REFERENCES shift_requests(id),
    company_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    start_time VARCHAR(255) NOT NULL,
    end_time VARCHAR(255) NOT NULL,
    action VARCHAR(20) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    attempts INTEGER DEFAULT 0,
    error_message TEXT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    processed_at TIMESTAMP NULL
);

CREATE INDEX IF NOT EXISTS idx_shift_requests_status ON shift_requests(status);
CREATE INDEX IF NOT EXISTS idx_individual_shifts_request_id ON individual_shifts(request_id);
CREATE INDEX IF NOT EXISTS idx_individual_shifts_status ON individual_shifts(status);
