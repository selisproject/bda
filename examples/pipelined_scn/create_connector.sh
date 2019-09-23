#!/bin/bash

curl -ik -X POST -H "Content-type:application/json" -H "Accept:application/json" --data @newconnector.json http://localhost:9999/api/connectors && echo
