#!/bin/bash

SELIS_SRC_DIR="$(dirname "$PWD")"
SELIS_NETWORK="selis-network"
SELIS_POSTGRES_VOLUME="selis-postgres-volume"
SELIS_HBASE_VOLUME="selis-hbase-volume"

SELIS_BDA_IMAGE="selis-bda-image:latest"
SELIS_JDK_IMAGE="openjdk:latest"
SELIS_POSTGRES_IMAGE="postgres:latest"
SELIS_HBASE_IMAGE="dajobe/hbase"

SELIS_BDA_CONTAINER="selis-controller"
SELIS_HBASE_CONTAINER="selis-hbase"
SELIS_POSTGRES_CONTAINER="selis-postgres"

SELIS_POSTGRES_DB=selisdb
SELIS_POSTGRES_USER=selis
SELIS_POSTGRES_PASSWORD=123456

################################################################################
# Pull images, if they do not exist. ###########################################
################################################################################

SELIS_JDK_IMAGE_ID="$(docker images --quiet "$SELIS_JDK_IMAGE")"

if [ "$SELIS_JDK_IMAGE_ID" == "" ]
then
    echo "Pulling openjdk image..."

    docker pull "$SELIS_JDK_IMAGE"
fi

SELIS_POSTGRES_IMAGE_ID="$(docker images --quiet "$SELIS_POSTGRES_IMAGE")"

if [ "$SELIS_POSTGRES_IMAGE_ID" == "" ]
then
    echo "Pulling postgres image..."

    docker pull "$SELIS_POSTGRES_IMAGE"
fi

SELIS_HBASE_IMAGE_ID="$(docker images --quiet "$SELIS_HBASE_IMAGE")"
if [ "$SELIS_HBASE_IMAGE_ID" == "" ]
then
    echo "Pulling hbase image..."

    docker pull "$SELIS_HBASE_IMAGE"
fi


################################################################################
# Create network, if it does not exist. ########################################
################################################################################

SELIS_NETWORK_ID="$(docker network list \
                        --filter name="$SELIS_NETWORK" \
                        --quiet)"

if [ "$SELIS_NETWORK_ID" == "" ]
then
    echo "Creating selis networks..."

    docker network create --driver bridge "$SELIS_NETWORK"
fi

################################################################################
# Create hbase/postgres volumes, if they do not exist. #########################
################################################################################

SELIS_HBASE_VOLUME_EXISTS=0
SELIS_POSTGRES_VOLUME_EXISTS=0
for i in $(docker volume list | awk '{ print $2 }')
do
    if [ "$i" == "$SELIS_POSTGRES_VOLUME" ]
    then
        SELIS_POSTGRES_VOLUME_EXISTS=1
    fi

    if [ "$i" == "$SELIS_HBASE_VOLUME" ]
    then
        SELIS_HBASE_VOLUME_EXISTS=1
    fi
done

if [ "$SELIS_POSTGRES_VOLUME_EXISTS" -eq 0 ]
then
    echo "Creating postgres volume..."

    docker volume create "$SELIS_POSTGRES_VOLUME"
fi

if [ "$SELIS_HBASE_VOLUME_EXISTS" -eq 0 ]
then
    echo "Creating hbase volume..."

    docker volume create "$SELIS_HBASE_VOLUME"
fi

################################################################################
# Build BDA containers. ########################################################
################################################################################

echo "Building selis container..."

docker build \
    --build-arg localuser="$(whoami)" \
    --tag "$SELIS_BDA_IMAGE" \
    .

################################################################################
# Run containers. ##############################################################
################################################################################

if [ "$1" == "run" ]
then
    if [ "$2" == "postgres" ]
    then
        echo "Running selis postgres container..."

        docker run \
            --detach \
            --network "$SELIS_NETWORK" \
            --volume "$SELIS_POSTGRES_VOLUME":/var/lib/postgresql/data \
            --env POSTGRES_USER="$SELIS_POSTGRES_USER" \
            --env POSTGRES_PASSWORD="$SELIS_POSTGRES_PASSWORD" \
            --env POSTGRES_DB="$SELIS_POSTGRES_DB" \
            --name "$SELIS_POSTGRES_CONTAINER" \
            "$SELIS_POSTGRES_IMAGE"
    fi

    if [ "$2" == "hbase" ]
    then
        echo "Running selis hbase container..."

        docker run \
            --detach \
            --network "$SELIS_NETWORK" \
            --volume "$SELIS_HBASE_VOLUME":/data \
            --name "$SELIS_HBASE_CONTAINER" \
            "$SELIS_HBASE_IMAGE"
    fi

    if [ "$2" == "controller" ]
    then
        echo "Running selis controller container."

        # src/main/scripts/selis-bda-server.sh

        docker run \
            --tty \
            --interactive \
            --network "$SELIS_NETWORK" \
            --volume "$SELIS_SRC_DIR":/code \
            --publish 127.0.0.1:9999:9999 \
            --name "$SELIS_BDA_CONTAINER" \
            "$SELIS_BDA_IMAGE"
    fi
fi
