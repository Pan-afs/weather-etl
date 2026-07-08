CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(50),
    temperature NUMERIC,
    weather_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);