CREATE USER selis WITH PASSWORD '123456';

CREATE DATABASE selis_bda_db WITH OWNER selis;

CREATE DATABASE selis_lab_db WITH OWNER selis;

CREATE DATABASE selis_kpi_db WITH OWNER selis;

CREATE DATABASE selis_test_db WITH OWNER selis;

\connect selis_bda_db

CREATE TABLE message_type (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(64) NOT NULL UNIQUE,
    description VARCHAR(256),
    active      BOOLEAN DEFAULT(true),
    format      VARCHAR
);

CREATE TABLE recipes (
    id                  SERIAL PRIMARY KEY,
    name                VARCHAR(64) NOT NULL UNIQUE,
    description         VARCHAR(256),
    executable_path     VARCHAR(512) NOT NULL UNIQUE,
    structure           JSONB,
    engine_args         JSONB,
    executable_args     JSONB
);

CREATE TABLE jobs (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(64) NOT NULL UNIQUE,
    description     VARCHAR(256),
    message_type_id INTEGER REFERENCES message_type(id),
    recipe_id       INTEGER REFERENCES recipes(id),
    job_type        VARCHAR(20),
    active          BOOLEAN DEFAULT(true)
);

CREATE TABLE execution_engines (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(64) NOT NULL UNIQUE,
    engine_path     TEXT,
    local_engine    BOOLEAN DEFAULT(true)
);

ALTER TABLE jobs                OWNER TO selis;
ALTER TABLE recipes             OWNER TO selis;
ALTER TABLE message_type        OWNER TO selis;
ALTER TABLE execution_engines   OWNER TO selis;

INSERT INTO execution_engines (name, engine_path, local_engine)
    VALUES
        ("python3", "/usr/bin/python3", true);
