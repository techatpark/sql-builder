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
