-- Create Core Tables for Device Registry
CREATE TABLE organizations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sites (
    id SERIAL PRIMARY KEY,
    org_id INT REFERENCES organizations(id),
    name VARCHAR(255) NOT NULL,
    location VARCHAR(255),
    timezone VARCHAR(50) DEFAULT 'UTC',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE assets (
    id VARCHAR(100) PRIMARY KEY,
    site_id INT REFERENCES sites(id),
    name VARCHAR(255) NOT NULL,
    device_class VARCHAR(100) NOT NULL,
    temp_threshold DOUBLE PRECISION DEFAULT 85.0,
    vibration_threshold DOUBLE PRECISION DEFAULT 4.5,
    calibration_constant DOUBLE PRECISION DEFAULT 1.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE planned_outages (
    id SERIAL PRIMARY KEY,
    asset_id VARCHAR(100) REFERENCES assets(id),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    description VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE alerts_history (
    id SERIAL PRIMARY KEY,
    asset_id VARCHAR(100) REFERENCES assets(id),
    rule_id VARCHAR(100) NOT NULL,
    metric_name VARCHAR(100),
    triggered_value DOUBLE PRECISION,
    threshold_value DOUBLE PRECISION,
    severity VARCHAR(50) DEFAULT 'CRITICAL',
    event_timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed Registry
INSERT INTO organizations (name) VALUES ('Omni Manufacturing Corp');
INSERT INTO sites (org_id, name, location, timezone) VALUES (1, 'Factory Alpha - Austin', 'Austin, TX', 'US/Central');
INSERT INTO sites (org_id, name, location, timezone) VALUES (1, 'Factory Beta - Berlin', 'Berlin, Germany', 'Europe/Berlin');

INSERT INTO assets (id, site_id, name, device_class, temp_threshold, vibration_threshold, calibration_constant)
VALUES ('austin-pump-01', 1, 'Main Coolant Pump 01', 'PUMP', 80.0, 3.8, 1.02);
INSERT INTO assets (id, site_id, name, device_class, temp_threshold, vibration_threshold, calibration_constant)
VALUES ('austin-compressor-02', 1, 'Air Compressor 02', 'COMPRESSOR', 95.0, 5.0, 0.98);
INSERT INTO assets (id, site_id, name, device_class, temp_threshold, vibration_threshold, calibration_constant)
VALUES ('berlin-turbine-03', 2, 'Steam Turbine 03', 'TURBINE', 110.0, 6.5, 1.00);
