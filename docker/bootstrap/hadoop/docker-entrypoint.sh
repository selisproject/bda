#!/bin/bash

if [ "$1" == "master" ]
then
    ## Start HDFS daemons
    if [ "$2" == "first" ]
    then
        # Format the namenode directory (DO THIS ONLY ONCE, THE FIRST TIME)
        # ONLY ON THE NAMENODE NODE
        echo "Formating namenode root fs."
        $HADOOP_HOME/bin/hdfs namenode -format
    fi

    # Start the namenode daemon
    # ONLY ON THE NAMENODE NODE
    echo "Starting HDFS namenode daemon."
    $HADOOP_HOME/sbin/hadoop-daemon.sh start namenode

    ## Start YARN daemons
    # Start the resourcemanager daemon
    # ONLY ON THE RESOURCEMANAGER NODE
    echo "Starting YARN resource manager daemon."
    $HADOOP_HOME/sbin/yarn-daemon.sh start resourcemanager

    # ...... . . .. ....
    echo "Sleeping ..."
    sleep infinity
fi

if [ "$1" == "worker" ]
then
    # Start the datanode daemon
    # ON ALL SLAVES
    echo "Starting HDFS datanode daemon."
    $HADOOP_HOME/sbin/hadoop-daemon.sh start datanode

    # ...... . . .. ....
    echo "Sleeping ..."
    sleep infinity
fi
