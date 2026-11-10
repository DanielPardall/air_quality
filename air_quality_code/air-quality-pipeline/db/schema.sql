

CREATE TABLE IF NOT EXISTS air_raw (
    id           BIGSERIAL PRIMARY KEY,
    sensor_id    INT NOT NULL,
    location_id  INT,
    parameter    TEXT NOT NULL,
    unit         TEXT,
    value        DOUBLE PRECISION,
    datetime_utc TIMESTAMP NOT NULL,
    ingested_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_air_raw_sensor_date
    ON air_raw (sensor_id, datetime_utc);



CREATE TABLE IF NOT EXISTS air_daily (
    id               BIGSERIAL PRIMARY KEY,
    date             DATE NOT NULL,
    parameter        TEXT NOT NULL,
    avg_value        DOUBLE PRECISION,
    max_value        DOUBLE PRECISION,
    min_value        DOUBLE PRECISION,
    num_hours        INT,
    sensor_id        INT,
    location_id      INT
);

CREATE INDEX IF NOT EXISTS idx_air_daily_date_param
    ON air_daily (date, parameter);



CREATE TABLE IF NOT EXISTS air_monthly (
    id               BIGSERIAL PRIMARY KEY,
    month_date       DATE NOT NULL,     
    parameter        TEXT NOT NULL,
    avg_value        DOUBLE PRECISION,
    max_value        DOUBLE PRECISION,
    min_value        DOUBLE PRECISION,
    num_hours        INT,
    sensor_id        INT,
    location_id      INT
);

CREATE INDEX IF NOT EXISTS idx_air_monthly_month_param
    ON air_monthly (month_date, parameter);


CREATE TABLE IF NOT EXISTS air_alerts (
    id               BIGSERIAL PRIMARY KEY,
    date             DATE NOT NULL,
    parameter        TEXT NOT NULL,
    threshold        DOUBLE PRECISION NOT NULL,
    num_exceedances  INT,               
    max_value        DOUBLE PRECISION,  
    sensor_id        INT,
    location_id      INT
);

CREATE INDEX IF NOT EXISTS idx_air_alerts_date_param
    ON air_alerts (date, parameter);
