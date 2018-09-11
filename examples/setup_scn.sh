#!/bin/bash

curl -ik -X POST -H "Content-type:application/json" -H "Accept:application/json" --data @newscn.json http://localhost:9999/api/datastore/create

curl -ik -X POST -H "Content-type:application/json" -H "Accept:application/json" --data @master_data.json http://localhost:9999/api/datastore/scn_slug/boot

curl -ik -X PUT -H "Content-type:application/json" -H "Accept:application/json" --data @msgtype.json http://localhost:9999/api/message/scn_slug

curl -ik -X PUT -H "Content-type:application/json" -d @recipe.json http://localhost:9999/api/recipe/scn_slug/

curl -ik -X PUT -H "Content-type:application/octet-stream" --data-binary @pi.py http://localhost:9999/api/recipe/scn_slug/upload/1/pi.py
