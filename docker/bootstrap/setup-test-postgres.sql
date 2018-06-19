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

INSERT INTO execution_engines (name, engine_path, local_engine, args)
    VALUES ('python3', '/usr/bin/python3', true, '{}'::json);
