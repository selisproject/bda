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

    # Start the nodemanager daemon
    # ON ALL SLAVES
    $HADOOP_PREFIX/sbin/yarn-daemon.sh start nodemanager

    # ...... . . .. ....
    sleep infinity
fi

if [ "$1" == "test-yarn" ]
then
    $HADOOP_PREFIX/bin/hadoop jar \
        $HADOOP_PREFIX/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-2.9.1.jar \
        org.apache.hadoop.yarn.applications.distributedshell.Client \
        --jar $HADOOP_PREFIX/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-2.9.1.jar \
        --shell_command date \
        --num_containers 2
fi

if [ "$1" == "test-spark" ]
then
    export SPARK_DIST_CLASSPATH=$($HADOOP_PREFIX/bin/hadoop classpath)

    $SPARK_HOME/bin/spark-submit \
        --class org.apache.spark.examples.SparkPi \
        --master yarn \
        --deploy-mode cluster \
        --driver-memory 4g \
        --executor-memory 2g \
        --executor-cores 1 \
        $SPARK_HOME/examples/jars/spark-examples*.jar \
        10
fi
