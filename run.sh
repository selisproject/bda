#!/bin/bash

export SPARK_DIST_CLASSPATH=$($HADOOP_PREFIX/bin/hadoop classpath)

cd bda-controller

/bin/sh -c ./src/main/scripts/selis-bda-server.sh
