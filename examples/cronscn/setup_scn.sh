#!/bin/bash

curl -ik -X POST -H "Content-type:application/json" -H "Accept:application/json" --data @newscn.json http://localhost:9999/api/datastore

curl -ik -X POST -H "Content-type:application/json" -H "Accept:application/json" --data @master_data.json http://localhost:9999/api/datastore/scn_slug/boot

curl -ik -X POST -H "Content-type:application/json" -H "Accept:application/json" --data @msgtype.json http://localhost:9999/api/messages/scn_slug

curl -ik -X POST -H "Content-type:application/json" -d @recipe.json http://localhost:9999/api/recipes/scn_slug/

curl -ik -X PUT -H "Content-type:application/octet-stream" --data-binary @recipe.py http://localhost:9999/api/recipes/scn_slug/1/recipe.py
