#!/bin/bash

SELIS_SRC_DIR="$(dirname "$PWD")"
SELIS_NETWORK="selis-network"
SELIS_POSTGRES_VOLUME="selis-postgres-volume"
SELIS_HBASE_VOLUME="selis-hbase-volume"

SELIS_BDA_DOCKERFILE="Dockerfile.bda"
SELIS_POSTGRES_DOCKERFILE="Dockerfile.postgres"
SELIS_HBASE_DOCKERFILE="Dockerfile.hbase"

SELIS_BDA_IMAGE="selis-bda-image:latest"
SELIS_POSTGRES_IMAGE="selis-postgres:latest"
SELIS_HBASE_IMAGE="selis-hbase:latest"

SELIS_JDK_PULL_IMAGE="openjdk:latest"
SELIS_POSTGRES_PULL_IMAGE="postgres:latest"
SELIS_HBASE_PULL_IMAGE="dajobe/hbase"

SELIS_BDA_CONTAINER="selis-controller"
SELIS_HBASE_CONTAINER="selis-hbase"
SELIS_POSTGRES_CONTAINER="selis-postgres"

################################################################################
# Pull images, if they do not exist. ###########################################
################################################################################

SELIS_JDK_IMAGE_ID="$(docker images --quiet "$SELIS_JDK_PULL_IMAGE")"

if [ "$SELIS_JDK_IMAGE_ID" == "" ]
then
    echo "Pulling openjdk image..."

    docker pull "$SELIS_JDK_PULL_IMAGE"
fi

SELIS_POSTGRES_IMAGE_ID="$(docker images --quiet "$SELIS_POSTGRES_PULL_IMAGE")"

if [ "$SELIS_POSTGRES_IMAGE_ID" == "" ]
then
    echo "Pulling postgres image..."

    docker pull "$SELIS_POSTGRES_PULL_IMAGE"
fi

SELIS_HBASE_IMAGE_ID="$(docker images --quiet "$SELIS_HBASE_PULL_IMAGE")"
if [ "$SELIS_HBASE_IMAGE_ID" == "" ]
then
    echo "Pulling hbase image..."

    docker pull "$SELIS_HBASE_PULL_IMAGE"
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
# Build images. ################################################################
################################################################################

SELIS_BDA_IMAGE_ID="$(docker images --quiet "$SELIS_BDA_IMAGE")"

if [ "$SELIS_BDA_IMAGE_ID" == "" ]
then
    echo "Building selis image..."

    docker build \
        --file "$SELIS_BDA_DOCKERFILE" \
        --build-arg localuser="$(whoami)" \
        --tag "$SELIS_BDA_IMAGE" \
        .
fi

SELIS_POSTGRES_IMAGE_ID="$(docker images --quiet "$SELIS_POSTGRES_IMAGE")"

if [ "$SELIS_POSTGRES_IMAGE_ID" == "" ]
then
    echo "Building postgres image..."

    docker build \
        --file "$SELIS_POSTGRES_DOCKERFILE" \
        --tag "$SELIS_POSTGRES_IMAGE" \
        ../bda-bootstrap/
fi

SELIS_HBASE_IMAGE_ID="$(docker images --quiet "$SELIS_HBASE_IMAGE")"

if [ "$SELIS_HBASE_IMAGE_ID" == "" ]
then
    echo "Building hbase image..."

    docker build \
        --file "$SELIS_HBASE_DOCKERFILE" \
        --tag "$SELIS_HBASE_IMAGE" \
        ../bda-bootstrap/
fi

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
