#!/bin/bash

# Format the namenode directory.
if [[ ! -d "$HADOOP_NAMENODE_DIR" ]]; then
    echo "Formating namenode root fs."
    $HADOOP_HOME/bin/hdfs namenode -format
fi

# Start the namenode.
echo "Starting HDFS namenode."
$HADOOP_HOME/bin/hdfs namenode
