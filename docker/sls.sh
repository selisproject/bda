#!/bin/bash

SELIS_SRC_DIR="$(dirname "$PWD")"

SELIS_NETWORK="selis-network"
SELIS_POSTGRES_VOLUME="selis-postgres-volume"
SELIS_HBASE_VOLUME="selis-hbase-volume"

SELIS_BDA_DOCKERFILE="Dockerfile.bda"
SELIS_POSTGRES_DOCKERFILE="Dockerfile.postgres"
SELIS_HBASE_DOCKERFILE="Dockerfile.hbase"
SELIS_SPARK_DOCKERFILE="Dockerfile.spark"

SELIS_BDA_IMAGE="selis-bda-image:latest"
SELIS_POSTGRES_IMAGE="selis-postgres:latest"
SELIS_HBASE_IMAGE="selis-hbase:latest"
SELIS_SPARK_IMAGE="selis-spark:latest"

SELIS_JDK_PULL_IMAGE="openjdk:8"
SELIS_POSTGRES_PULL_IMAGE="postgres:latest"
SELIS_HBASE_PULL_IMAGE="dajobe/hbase:latest"
SELIS_KEYCLOAK_PULL_IMAGE="jboss/keycloak:latest"
SELIS_PUBSUB_PULL_IMAGE="tudselis/pubsub:dev-1803071815"

SELIS_BDA_CONTAINER="selis-controller"
SELIS_HBASE_CONTAINER="selis-hbase"
SELIS_POSTGRES_CONTAINER="selis-postgres"
SELIS_KEYCLOAK_CONTAINER="selis-keycloak"
SELIS_SPARK_MASTER_CONTAINER="selis-spark-master"
SELIS_SPARK_WORKER_CONTAINER_0="selis-spark-worker-0"
SELIS_SPARK_WORKER_CONTAINER_1="selis-spark-worker-1"
SELIS_PUBSUB_CONTAINER="selis-pubsub"


################################################################################
# Clean all. ###################################################################
################################################################################

if [ "$1" == "clean" ]
then
    echo "Running clean all process..."

    docker rm "$SELIS_BDA_CONTAINER"
    docker rm "$SELIS_HBASE_CONTAINER"
    docker rm "$SELIS_POSTGRES_CONTAINER"
    docker rm "$SELIS_KEYCLOAK_CONTAINER"
    docker rm "$SELIS_SPARK_MASTER_CONTAINER"
    docker rm "$SELIS_SPARK_WORKER_CONTAINER_0"
    docker rm "$SELIS_SPARK_WORKER_CONTAINER_1"
    docker rm "$SELIS_PUBSUB_CONTAINER"

    docker rmi "$SELIS_BDA_IMAGE"
    docker rmi "$SELIS_POSTGRES_IMAGE"
    docker rmi "$SELIS_HBASE_IMAGE"
    docker rmi "$SELIS_SPARK_IMAGE"

    docker volume rm "$SELIS_POSTGRES_VOLUME"
    docker volume rm "$SELIS_HBASE_VOLUME"

    docker network rm "$SELIS_NETWORK"

    exit
fi

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

SELIS_KEYCLOAK_IMAGE_ID="$(docker images --quiet "$SELIS_KEYCLOAK_PULL_IMAGE")"
if [ "$SELIS_KEYCLOAK_IMAGE_ID" == "" ]
then
    echo "Pulling keycloak image..."

    docker pull "$SELIS_KEYCLOAK_PULL_IMAGE"
fi

SELIS_PUBSUB_IMAGE_ID="$(docker images --quiet "$SELIS_PUBSUB_PULL_IMAGE")"
if [ "$SELIS_PUBSUB_IMAGE_ID" == "" ]
then
    echo "Pulling pubsub image..."

    docker pull "$SELIS_PUBSUB_PULL_IMAGE"
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

SELIS_POSTGRES_IMAGE_ID="$(docker images --quiet "$SELIS_POSTGRES_IMAGE")"

if [ "$SELIS_POSTGRES_IMAGE_ID" == "" ]
then
    echo "Building postgres image..."

    docker build \
        --file "$SELIS_POSTGRES_DOCKERFILE" \
        --tag "$SELIS_POSTGRES_IMAGE" \
        .
fi

SELIS_HBASE_IMAGE_ID="$(docker images --quiet "$SELIS_HBASE_IMAGE")"

if [ "$SELIS_HBASE_IMAGE_ID" == "" ]
then
    echo "Building hbase image..."

    docker build \
        --file "$SELIS_HBASE_DOCKERFILE" \
        --tag "$SELIS_HBASE_IMAGE" \
        .
fi

SELIS_SPARK_IMAGE_ID="$(docker images --quiet "$SELIS_SPARK_IMAGE")"

if [ "$SELIS_SPARK_IMAGE_ID" == "" ]
then
    echo "Building spark image..."

    docker build \
        --file "$SELIS_SPARK_DOCKERFILE" \
        --tag "$SELIS_SPARK_IMAGE" \
        .
fi

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

################################################################################
# Run containers. ##############################################################
################################################################################

if [ "$1" == "run" ]
then
    if [ "$2" == "postgres" ] || [ "$2" == "all" ]
    then
        echo "Running selis postgres container..."

        docker run \
            --detach \
            --network "$SELIS_NETWORK" \
            --volume "$SELIS_POSTGRES_VOLUME":/var/lib/postgresql/data \
            --hostname "$SELIS_POSTGRES_CONTAINER" \
            --name "$SELIS_POSTGRES_CONTAINER" \
            "$SELIS_POSTGRES_IMAGE"
    fi

    if [ "$2" == "hbase" ] || [ "$2" == "all" ]
    then
        echo "Running selis hbase container..."

        docker run \
            --detach \
            --network "$SELIS_NETWORK" \
            --volume "$SELIS_HBASE_VOLUME":/data \
            --hostname "$SELIS_HBASE_CONTAINER" \
            --name "$SELIS_HBASE_CONTAINER" \
            "$SELIS_HBASE_IMAGE"

        # echo "Creating 'Events' table, if it does not exist..."

        # docker exec \
        #     selis-hbase \
        #     /bootstrap-hbase.d/bootstrap-hbase.sh
    fi

   if [ "$2" == "keycloak" ] || [ "$2" == "all" ]
   then
        echo "Running selis keycloak container."

        docker run \
            --detach \
            --network "$SELIS_NETWORK" \
            --publish 127.0.0.1:8989:8080 \
            --env DB_VENDOR=H2 \
            --env KEYCLOAK_USER=selis-admin \
            --env KEYCLOAK_PASSWORD=123456 \
            --hostname "$SELIS_KEYCLOAK_CONTAINER" \
            --name "$SELIS_KEYCLOAK_CONTAINER" \
            "$SELIS_KEYCLOAK_PULL_IMAGE"
    fi

    if [ "$2" == "spark" ] || [ "$2" == "all" ]
    then
        echo "Running selis spark master container."

        docker run \
            --detach \
            --network "$SELIS_NETWORK" \
            --publish 127.0.0.1:8080:8080 \
            --hostname "$SELIS_SPARK_MASTER_CONTAINER" \
            --name "$SELIS_SPARK_MASTER_CONTAINER" \
            "$SELIS_SPARK_IMAGE" \
            /entrypoint.sh master first

        echo "Running selis spark worker container."

        docker run \
            --detach \
            --network "$SELIS_NETWORK" \
            --publish 127.0.0.1:8081:8081 \
            --hostname "$SELIS_SPARK_WORKER_CONTAINER_0" \
            --name "$SELIS_SPARK_WORKER_CONTAINER_0" \
            "$SELIS_SPARK_IMAGE" \
            /entrypoint.sh worker

        docker run \
            --detach \
            --network "$SELIS_NETWORK" \
            --publish 127.0.0.1:8082:8082 \
            --hostname "$SELIS_SPARK_WORKER_CONTAINER_1" \
            --name "$SELIS_SPARK_WORKER_CONTAINER_1" \
            "$SELIS_SPARK_IMAGE" \
            /entrypoint.sh worker
    fi

    if [ "$2" == "pubsub" ] || [ "$2" == "all" ]
    then
        echo "Running pubsub container."

        docker run \
            --detach \
            --network "$SELIS_NETWORK" \
            --name "$SELIS_PUBSUB_CONTAINER" \
            --hostname "$SELIS_PUBSUB_CONTAINER" \
            --publish 127.0.0.1:20000:20000 \
            --publish 127.0.0.1:20001:20001 \
            "$SELIS_PUBSUB_PULL_IMAGE"
    fi

    if [ "$2" == "controller" ] || [ "$2" == "all" ]
    then
        echo "Running selis controller container."

        docker run \
            --tty \
            --interactive \
            --network "$SELIS_NETWORK" \
            --volume "$SELIS_SRC_DIR":/code \
            --publish 127.0.0.1:9999:9999 \
            --hostname "$SELIS_BDA_CONTAINER" \
            --name "$SELIS_BDA_CONTAINER" \
            "$SELIS_BDA_IMAGE"
   fi
fi

################################################################################
# Start containers. ############################################################
################################################################################

if [ "$1" == "startall" ]
then
    echo "Starting all containers..."

    docker start "$SELIS_HBASE_CONTAINER"
    docker start "$SELIS_POSTGRES_CONTAINER"
    docker start "$SELIS_KEYCLOAK_CONTAINER"
    docker start "$SELIS_SPARK_MASTER_CONTAINER"
    docker start "$SELIS_SPARK_WORKER_CONTAINER_0"
    docker start "$SELIS_SPARK_WORKER_CONTAINER_1"
    docker start "$SELIS_PUBSUB_CONTAINER"
    docker start "$SELIS_BDA_CONTAINER"

fi


################################################################################
# Stop containers. #############################################################
################################################################################

if [ "$1" == "stopall" ]
then
    echo "Stopping all containers..."

    docker stop "$SELIS_BDA_CONTAINER"
    docker stop "$SELIS_PUBSUB_CONTAINER"
    docker stop "$SELIS_HBASE_CONTAINER"
    docker stop "$SELIS_POSTGRES_CONTAINER"
    docker stop "$SELIS_KEYCLOAK_CONTAINER"
    docker stop "$SELIS_SPARK_MASTER_CONTAINER"
    docker stop "$SELIS_SPARK_WORKER_CONTAINER_0"
    docker stop "$SELIS_SPARK_WORKER_CONTAINER_1"

fi
