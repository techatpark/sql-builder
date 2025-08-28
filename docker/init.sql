-- Drop tables if they exist
DROP TABLE IF EXISTS movie;
DROP TABLE IF EXISTS director;
DROP TABLE IF EXISTS alltypes;

-- Create movie table
CREATE TABLE movie (
    id BIGSERIAL PRIMARY KEY,
    title VARCHAR(80) NOT NULL,
    directed_by VARCHAR(80)
);

CREATE TABLE director (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(80) UNIQUE NOT NULL
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

DROP PROCEDURE IF EXISTS insert_alltypes_in;

CREATE PROCEDURE insert_alltypes_in(
    IN p_str VARCHAR,
    IN p_intval INTEGER,
    IN p_longval BIGINT,
    IN p_doubleval DOUBLE PRECISION,
    IN p_floatval REAL,
    IN p_boolval BOOLEAN,
    IN p_shortval SMALLINT,
    IN p_byteval SMALLINT,
    IN p_dateval DATE,
    IN p_timeval TIME,
    IN p_timestampval TIMESTAMP,
    IN p_bigdecimalval DECIMAL(15, 2),
    IN p_bytesval BYTEA,
    IN p_urlval VARCHAR,
    IN p_nullval VARCHAR
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO alltypes (
        str, intval, longval, doubleval, floatval, boolval,
        shortval, byteval, dateval, timeval, timestampval, bigdecimalval,
        bytesval, urlval, nullval
    )
    VALUES (
        p_str, p_intval, p_longval, p_doubleval, p_floatval, p_boolval,
        p_shortval, p_byteval, p_dateval, p_timeval, p_timestampval, p_bigdecimalval,
        p_bytesval, p_urlval, p_nullval
    );
END;
$$;

DROP PROCEDURE IF EXISTS insert_alltypes_out;

CREATE PROCEDURE insert_alltypes_out(
    INOUT p_str VARCHAR,
    INOUT p_intval INTEGER,
    INOUT p_longval BIGINT,
    INOUT p_doubleval DOUBLE PRECISION,
    INOUT p_floatval REAL,
    INOUT p_boolval BOOLEAN,
    INOUT p_shortval SMALLINT,
    INOUT p_byteval SMALLINT,
    INOUT p_dateval DATE,
    INOUT p_timeval TIME,
    INOUT p_timestampval TIMESTAMP,
    INOUT p_bigdecimalval DECIMAL(15, 2),
    INOUT p_bytesval BYTEA,
    INOUT p_urlval VARCHAR,
    INOUT p_nullval VARCHAR
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO alltypes (
        str, intval, longval, doubleval, floatval, boolval,
        shortval, byteval, dateval, timeval, timestampval, bigdecimalval,
        bytesval, urlval, nullval
    )
    VALUES (
        p_str, p_intval, p_longval, p_doubleval, p_floatval, p_boolval,
        p_shortval, p_byteval, p_dateval, p_timeval, p_timestampval, p_bigdecimalval,
        p_bytesval, p_urlval, p_nullval
    );
END;
$$;

DROP PROCEDURE IF EXISTS insert_alltypes_in_and_out;

CREATE PROCEDURE insert_alltypes_in_and_out(
    OUT p_str_out VARCHAR,
    IN p_str VARCHAR,
    IN p_intval INTEGER,
    IN p_longval BIGINT,
    IN p_doubleval DOUBLE PRECISION,
    IN p_floatval REAL,
    IN p_boolval BOOLEAN,
    IN p_shortval SMALLINT,
    IN p_byteval SMALLINT,
    IN p_dateval DATE,
    IN p_timeval TIME,
    IN p_timestampval TIMESTAMP,
    IN p_bigdecimalval DECIMAL(15, 2),
    IN p_bytesval BYTEA,
    IN p_urlval VARCHAR,
    IN p_nullval VARCHAR,
    OUT p_shortval_out SMALLINT
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO alltypes (
        str, intval, longval, doubleval, floatval, boolval,
        shortval, byteval, dateval, timeval, timestampval, bigdecimalval,
        bytesval, urlval, nullval
    )
    VALUES (
        p_str, p_intval, p_longval, p_doubleval, p_floatval, p_boolval,
        p_shortval, p_byteval, p_dateval, p_timeval, p_timestampval, p_bigdecimalval,
        p_bytesval, p_urlval, p_nullval
    );

    -- Return the input string as output
    p_str_out := p_str;
    p_shortval_out := p_shortval;
END;
$$;

DROP PROCEDURE IF EXISTS insert_movie_in;

CREATE PROCEDURE insert_movie_in(
    IN p_title TEXT,
    IN p_director TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO movie (title, directed_by)
    VALUES (p_title, p_director);
END;
$$;


DROP PROCEDURE IF EXISTS insert_movie_out;

CREATE PROCEDURE insert_movie_out(
    IN p_title TEXT,
    IN p_director TEXT,
    OUT p_id BIGINT
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO movie (title, directed_by)
    VALUES (p_title, p_director)
    RETURNING id INTO p_id;
END;
$$;

DROP FUNCTION IF EXISTS insert_movie_fn(TEXT, TEXT);

CREATE FUNCTION insert_movie_fn(
    p_title TEXT,
    p_director TEXT
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    new_id BIGINT;
BEGIN
    INSERT INTO movie (title, directed_by)
    VALUES (p_title, p_director)
    RETURNING id INTO new_id;

    RETURN new_id;
END;
$$;

DROP PROCEDURE IF EXISTS update_title_inout;

CREATE PROCEDURE update_title_inout(
    INOUT p_id BIGINT,
    INOUT p_title TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE movie
    SET title = p_title
    WHERE id = p_id;

    -- Confirm the updated title (in case it's modified by a trigger, etc.)
    SELECT title INTO p_title
    FROM movie
    WHERE id = p_id;
END;
$$;

CREATE OR REPLACE FUNCTION get_movie_by_id(p_id BIGINT)
RETURNS REFCURSOR AS $$
DECLARE
    ref refcursor;
BEGIN
    OPEN ref FOR
    SELECT id, title, directed_by
    FROM movie
    WHERE id = p_id;

    RETURN ref;
END;
$$ LANGUAGE plpgsql;

