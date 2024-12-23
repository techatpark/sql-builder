DROP TABLE movie IF EXISTS;

CREATE TABLE movie (
    id bigint auto_increment PRIMARY KEY,
    title VARCHAR(80) NOT NULL,
    directed_by VARCHAR(80)
);

DROP TABLE AllTypes IF EXISTS;

CREATE TABLE AllTypes (
    nullVal VARCHAR,
    str VARCHAR(255),
    intVal INT,
    longVal BIGINT,
    doubleVal DOUBLE,
    floatVal FLOAT,
    boolVal BOOLEAN,
    shortVal SMALLINT,
    byteVal TINYINT,
    dateVal DATE,
    timeVal TIME,
    timestampVal TIMESTAMP,
    bigDecimalVal DECIMAL(15, 2),
    bytesVal BLOB,
    urlVal VARCHAR(255)
);