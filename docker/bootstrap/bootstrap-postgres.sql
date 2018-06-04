CREATE USER selis WITH PASSWORD '123456';

CREATE DATABASE selis_bda_db WITH OWNER selis;

CREATE DATABASE selis_lab_db WITH OWNER selis;

CREATE DATABASE selis_test_db WITH OWNER selis;

\connect selis_bda_db

CREATE TABLE message_type (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(64) NOT NULL UNIQUE,
    description VARCHAR(256),
    active      BOOLEAN DEFAULT(true),
    format      VARCHAR
);

ALTER TABLE message_type OWNER TO selis;
