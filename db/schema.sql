-- Lookup Tables

CREATE TABLE IF NOT EXISTS incident_types (
    type_id INTEGER PRIMARY KEY AUTOINCREMENT,
    type_name TEXT NOT NULL UNIQUE,
    force_level INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS precincts (
    precinct_id INTEGER PRIMARY KEY AUTOINCREMENT,
    precinct_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS sectors (
    sector_id INTEGER PRIMARY KEY AUTOINCREMENT,
    sector_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS races (
    race_id INTEGER PRIMARY KEY AUTOINCREMENT,
    race_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS genders (
    gender_id INTEGER PRIMARY KEY AUTOINCREMENT,
    gender_name TEXT NOT NULL UNIQUE
);

-- Main Fact Table

CREATE TABLE IF NOT EXISTS incidents (
    uniqueid TEXT PRIMARY KEY,
    incident_num TEXT NOT NULL,
    type_id INTEGER NOT NULL REFERENCES incident_types(type_id),
    incident_date DATE,
    incident_time TEXT,
    incident_year INTEGER,
    day_of_week TEXT,
    hour_of_day INTEGER,
    precinct_id INTEGER REFERENCES precincts(precinct_id),
    sector_id INTEGER REFERENCES sectors(sector_id),
    beat TEXT,
    officer_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    race_id INTEGER REFERENCES races(race_id),
    gender_id INTEGER REFERENCES genders(gender_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_incidents_year ON incidents(incident_year);
CREATE INDEX IF NOT EXISTS idx_incidents_date ON incidents(incident_date);
CREATE INDEX IF NOT EXISTS idx_incidents_precinct ON incidents(precinct_id);
CREATE INDEX IF NOT EXISTS idx_incidents_type ON incidents(type_id);
CREATE INDEX IF NOT EXISTS idx_incidents_race ON incidents(race_id);
CREATE INDEX IF NOT EXISTS idx_incidents_officer ON incidents(officer_id);
CREATE INDEX IF NOT EXISTS idx_incidents_incident_num ON incidents(incident_num);

CREATE TABLE IF NOT EXISTS update_log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    records_fetched INTEGER,
    records_new INTEGER,
    records_updated INTEGER,
    errors TEXT,
    status TEXT
);
