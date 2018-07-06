CREATE USER selis WITH PASSWORD '123456';

CREATE USER selis_admin WITH PASSWORD '123456';

ALTER USER selis_admin CREATEDB;

GRANT selis to selis_admin;

CREATE DATABASE selis_bda_db WITH OWNER selis;

CREATE DATABASE selis_lab_db WITH OWNER selis;

CREATE DATABASE selis_kpi_db WITH OWNER selis;

CREATE DATABASE selis_test_db WITH OWNER selis;


\connect selis_bda_db

CREATE TABLE scn_db_info (
    id          SERIAL PRIMARY KEY,
    slug        VARCHAR(64) NOT NULL UNIQUE,
    name        VARCHAR(128) NOT NULL,
    description VARCHAR(256),
    dbname      VARCHAR(64) NOT NULL
);

CREATE TABLE message_type (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(64) NOT NULL UNIQUE,
    description VARCHAR(256),
    active      BOOLEAN DEFAULT(true),
    format      VARCHAR
);

CREATE TABLE execution_engines (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(64) NOT NULL UNIQUE,
    engine_path     TEXT,
    local_engine    BOOLEAN DEFAULT(true),
    args            JSONB
);

CREATE TABLE recipes (
    id                  SERIAL PRIMARY KEY,
    name                VARCHAR(64) NOT NULL UNIQUE,
    description         VARCHAR(256),
    executable_path     VARCHAR(512) NOT NULL UNIQUE,
    engine_id           INTEGER REFERENCES execution_engines(id),
    args                JSONB
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

ALTER TABLE jobs                OWNER TO selis;
ALTER TABLE recipes             OWNER TO selis;
ALTER TABLE scn_db_info         OWNER TO selis;
ALTER TABLE message_type        OWNER TO selis;
ALTER TABLE execution_engines   OWNER TO selis;

--grant all privileges on table message_type to selis;
--grant all privileges on table jobs to selis;
--grant all privileges on table recipes to selis;
--grant all privileges on table execution_engines to selis;


INSERT INTO execution_engines (name, engine_path, local_engine, args)
    VALUES ('python3', '/usr/bin/python3', true, '{}'::json);
INSERT INTO execution_engines (name, engine_path, local_engine, args)
    VALUES ('pyspark', 'spark://selis-spark-master:7077', false, '{}'::json);
