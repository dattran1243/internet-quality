ALTER TABLE telecom.incident_candidates
    ADD COLUMN IF NOT EXISTS country_code String DEFAULT 'VN'
    AFTER hour_start_utc;