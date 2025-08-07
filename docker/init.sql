-- Drop tables if they exist
DROP TABLE IF EXISTS movie;
DROP TABLE IF EXISTS alltypes;

-- Create movie table
CREATE TABLE movie (
    id BIGSERIAL PRIMARY KEY,
    title VARCHAR(80) NOT NULL,
    directed_by VARCHAR(80)
);

-- Create AllTypes table
CREATE TABLE alltypes (
    nullval VARCHAR,
    str VARCHAR(255),
    intval INTEGER,
    longval BIGINT,
    doubleval DOUBLE PRECISION,
    floatval REAL,
    boolval BOOLEAN,
    shortval SMALLINT,
    byteval SMALLINT,
    dateval DATE,
    timeval TIME,
    timestampval TIMESTAMP,
    bigdecimalval DECIMAL(15, 2),
    bytesval BYTEA,
    urlval VARCHAR(255)
);
