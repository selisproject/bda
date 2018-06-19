#!/bin/bash

mvn initialize

mvn verify

mvn package \
    --fail-fast \
    -Dconfiguration.path="$(pwd)/conf/bda.properties" \
    -Dpostgres.setup.path="$(pwd)/docker/bootstrap/setup-test-postgres.sql" \
    -Dpostgres.teardown.path="$(pwd)/docker/bootstrap/teardown-test-postgres.sql"
