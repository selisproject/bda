#!/bin/bash

curl -ik -X POST -H "Content-type:application/json" -H "Accept:application/json" --data @newscn.json http://localhost:9999/api/datastore

curl -ik -X POST -H "Content-type:application/json" -H "Accept:application/json" --data @master_data.json http://localhost:9999/api/datastore/pipe/boot

curl -ik -X POST -H "Content-type:application/json" -H "Accept:application/json" --data @msgtype1.json http://localhost:9999/api/messages/pipe

curl -ik -X POST -H "Content-type:application/json" -H "Accept:application/json" --data @msgtype2.json http://localhost:9999/api/messages/pipe

curl -ik -X POST -H "Content-type:application/json" -d @recipe1.json http://localhost:9999/api/recipes/pipe/

curl -ik -X PUT -H "Content-type:application/octet-stream" --data-binary @recipe1.py http://localhost:9999/api/recipes/pipe/1/recipe1.py

curl -ik -X POST -H "Content-type:application/json" -d @recipe2.json http://localhost:9999/api/recipes/pipe/

curl -ik -X PUT -H "Content-type:application/octet-stream" --data-binary @recipe2.py http://localhost:9999/api/recipes/pipe/2/recipe2.py

#curl -ik -X POST -H "Content-type:application/json" -H "Accept:application/json" --data @job1.json http://localhost:9999/api/jobs/pipe && echo

#curl -ik -X POST -H "Content-type:application/json" -H "Accept:application/json" --data @job2.json http://localhost:9999/api/jobs/pipe && echo

#curl --request POST --url https://selis-gw.cslab.ece.ntua.gr:20000/publish --header "Content-Type: application/json" --data @msg1.json 

#curl --request POST --url https://selis-gw.cslab.ece.ntua.gr:20000/publish --header "Content-Type: application/json" --data @msg2.json 

