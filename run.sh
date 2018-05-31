#!/bin/bash

mvn package
cd bda-controller
/bin/sh -c ./src/main/scripts/selis-bda-server.sh
