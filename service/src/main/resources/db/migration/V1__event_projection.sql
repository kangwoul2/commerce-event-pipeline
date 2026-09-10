CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS regional_sales (
    region VARCHAR(120) PRIMARY KEY,
    order_count BIGINT NOT NULL CHECK (order_count >= 0),
    total_amount NUMERIC(20,2) NOT NULL CHECK (total_amount >= 0)
);
