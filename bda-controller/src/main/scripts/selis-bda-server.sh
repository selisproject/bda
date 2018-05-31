#!/bin/bash

ROOT_DIR="target"
LIB_DIR="${ROOT_DIR}/lib"
#JAR_FILE="$(ls ${ROOT_DIR}/bda-controller-*.jar)"
JAR_FILE="$(echo ${ROOT_DIR}/classes)" # used for debugging only
ENTRYPOINT="gr.ntua.ece.cslab.selis.bda.controller.Entrypoint"
CONF_FILE="../conf/bda.properties"

CLASSPATH="$(ls $LIB_DIR/* | tr '\n' ':')$JAR_FILE"
java -cp $CLASSPATH $ENTRYPOINT $CONF_FILE
