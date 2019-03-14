CREATE USER selis WITH PASSWORD '123456';

CREATE USER selis_admin WITH PASSWORD '123456';

ALTER USER selis_admin CREATEDB;

GRANT selis to selis_admin;

CREATE DATABASE selis_bda_db WITH OWNER selis;

CREATE DATABASE selis_test_db WITH OWNER selis;


\connect selis_bda_db

CREATE TABLE scn_db_info (
    id           SERIAL PRIMARY KEY,
    slug         VARCHAR(64) NOT NULL UNIQUE,
    name         VARCHAR(128) NOT NULL,
    description  VARCHAR(256),
    dbname       VARCHAR(64) NOT NULL UNIQUE,
    connector_id INTEGER
);

ALTER TABLE scn_db_info OWNER TO selis;

CREATE TABLE execution_engines (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(64) NOT NULL UNIQUE,
    engine_path     TEXT,
    local_engine    BOOLEAN DEFAULT(true),
    args            JSONB
);

ALTER TABLE execution_engines OWNER TO selis;

INSERT INTO execution_engines (name, engine_path, local_engine, args)
    VALUES
	('python3', '/usr/bin/python3', true, '{}'::json),
	('spark', 'spark://selis-spark-master:7077', false, '{}'::json),
	('livy', 'http://selis-livy:8998', false, '{}'::json);

CREATE TABLE execution_languages (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(64) NOT NULL UNIQUE
);

ALTER TABLE execution_languages OWNER TO selis;

INSERT INTO execution_languages (name)
    VALUES
	('python');

CREATE TABLE connectors (
    id                 SERIAL PRIMARY KEY,
    name               VARCHAR(64) NOT NULL UNIQUE,
    address            VARCHAR(256) NOT NULL,
    port               VARCHAR(5) NOT NULL,
    metadata           JSON,
    is_external        BOOLEAN DEFAULT(false)
);

ALTER TABLE connectors OWNER TO selis;


\connect selis_test_db

CREATE TABLE scn_db_info (
    id           SERIAL PRIMARY KEY,
    slug         VARCHAR(64) NOT NULL UNIQUE,
    name         VARCHAR(128) NOT NULL,
    description  VARCHAR(256),
    dbname       VARCHAR(64) NOT NULL UNIQUE,
    connector_id INTEGER
);

ALTER TABLE scn_db_info OWNER TO selis;

CREATE TABLE execution_engines (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(64) NOT NULL UNIQUE,
    engine_path     TEXT,
    local_engine    BOOLEAN DEFAULT(true),
    args            JSONB
);

ALTER TABLE execution_engines OWNER TO selis;

INSERT INTO execution_engines (name, engine_path, local_engine, args)
    VALUES
	('python3', '/usr/bin/python3', true, '{}'::json),
	('spark', 'spark://selis-spark-master:7077', false, '{}'::json),
	('livy', 'http://selis-livy:8998', false, '{}'::json);

CREATE TABLE execution_languages (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(64) NOT NULL UNIQUE
);

ALTER TABLE execution_languages OWNER TO selis;

INSERT INTO execution_languages (name)
    VALUES
	('python');

CREATE TABLE connectors (
    id                 SERIAL PRIMARY KEY,
    name               VARCHAR(64) NOT NULL UNIQUE,
    address            VARCHAR(256) NOT NULL,
    port               VARCHAR(5) NOT NULL,
    metadata           JSON,
    is_external        BOOLEAN DEFAULT(false)
);

ALTER TABLE connectors OWNER TO selis;
