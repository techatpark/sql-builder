DROP TABLE movie IF EXISTS;

CREATE TABLE movie (
    id bigint auto_increment PRIMARY KEY,
    title VARCHAR(80) NOT NULL,
    directed_by VARCHAR(80)
);