#!/bin/bash

if [ "$1" == "nodemanager" ]
then
    # Start the nodemanager daemon
    # ON ALL SLAVES
    $HADOOP_PREFIX/sbin/yarn-daemon.sh start nodemanager

    if [ "$2" == "block" ]
    then
        # ...... . . .. ....
        sleep infinity
    fi
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
