#!/bin/bash

if [ "$1" == "master" ]
then
    ## Start HDFS daemons
    if [ "$2" == "first" ]
    then
        # Format the namenode directory (DO THIS ONLY ONCE, THE FIRST TIME)
        # ONLY ON THE NAMENODE NODE
        $HADOOP_PREFIX/bin/hdfs namenode -format
    fi

    # Start the namenode daemon
    # ONLY ON THE NAMENODE NODE
    $HADOOP_PREFIX/sbin/hadoop-daemon.sh start namenode

    ## Start YARN daemons
    # Start the resourcemanager daemon
    # ONLY ON THE RESOURCEMANAGER NODE
    $HADOOP_PREFIX/sbin/yarn-daemon.sh start resourcemanager

    # ...... . . .. ....
    sleep infinity
fi

if [ "$1" == "worker" ]
then
    # Start the datanode daemon
    # ON ALL SLAVES
    $HADOOP_PREFIX/sbin/hadoop-daemon.sh start datanode

    # ...... . . .. ....
    sleep infinity
fi
