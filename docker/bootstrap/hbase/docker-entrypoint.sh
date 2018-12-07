#!/bin/bash

# Write myid only if it does not exist.
if [ ! -f "$ZOOKEEPER_DATADIR/myid" ]; then
    echo "${ZOOKEEPER_MYID}" > "$ZOOKEEPER_DATADIR/myid"
fi

# Start zookeeper daemon (don't ask).
echo "Starting Zookeeper daemon."
$ZOOKEEPER_HOME/bin/zkServer.sh start

if [ "$1" == "master" ]
then
    # Start the hbase daemon.
    # ONLY ON THE MASTER NODE
    echo "Starting HBASE master daemon."
	$HBASE_HOME/bin/hbase-daemon.sh start master

    # ...... . . .. ....
    echo "Sleeping ..."
    sleep infinity
fi

if [ "$1" == "worker" ]
then
    # Start the hbase daemon
    # ON ALL WORKERS
    echo "Starting HBASE region server daemon."
	$HBASE_HOME/bin/hbase-daemon.sh start regionserver

    # ...... . . .. ....
    echo "Sleeping ..."
    sleep infinity
fi
