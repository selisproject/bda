ARG HADOOP_VERSION 
FROM selis/hadoop-yarn-nodemanager-${HADOOP_VERSION}

# Specify hadoop resources.
ARG HADOOP_PREFIX=/usr/local/hadoop

# Specify spark resources.
ARG SPARK_VERSION
ARG SPARK_PREFIX=/usr/local/spark
ARG SPARK_URL=https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz

RUN \
    # Install dependencies.
    DEBIAN_FRONTEND=noninteractive \
    apt-get install --yes --no-install-recommends \
        python3 \
        python3-psycopg2 \
        python3-dateutil \
        python3-requests \
        python3-numpy \
        python3-pandas && \
    # Clean.
    apt-get clean autoclean && \
    apt-get autoremove --yes && \
    # Create spark, resources directories.
    mkdir -p ${SPARK_PREFIX} /resources && \
    # Download postgresql jar.
    curl https://jdbc.postgresql.org/download/postgresql-42.2.1.jar --output /resources/postgresql-42.2.1.jar && \
    # Download spark gpg keys.
    curl https://dist.apache.org/repos/dist/release/spark/KEYS -o SPARK_KEYS && \
    gpg --import SPARK_KEYS && \
    # Download, install spark.
    curl -fSL "${SPARK_URL}" -o /tmp/spark.tgz && \
    curl -fSL "${SPARK_URL}.asc" -o /tmp/spark.tgz.asc && \
    gpg --verify /tmp/spark.tgz.asc && \
    tar -C "${SPARK_PREFIX}" --strip=1 -xzf /tmp/spark.tgz && \
    rm /tmp/spark.tgz*
    
# Add hbase configuration files.
ADD ./hbase-site.xml "${SPARK_PREFIX}/conf/"

# Set spark environment variables.
ENV SPARK_HOME "${SPARK_PREFIX}"
ENV PYSPARK_PYTHON "/usr/bin/python3"
