#!/bin/bash

TABLES=$(echo "list" | hbase shell)

EVENTS=$(echo "$TABLES" | grep -b -o "Events")

if [ "$EVENTS" == "" ]
then
    echo "create 'Events', 'messages'" | hbase shell
fi
