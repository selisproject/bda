ARG SPARK_VERSION
FROM selis/spark-${SPARK_VERSION}

# Specify bda resources.
ARG BDA_VERSION
ARG BDA_PREFIX

# TODO: This should be a URL to a release.
ARG BDA_URL="./bda-controller-${BDA_VERSION}-jars.tar.gz"

# Make directories.
RUN mkdir -p "${BDA_PREFIX}/jars" "${BDA_PREFIX}/conf" /uploads /results

# Add jars.
ADD "${BDA_URL}" "${BDA_PREFIX}/jars"
# RUN \ tar -xzf "/tmp/bda-controller-${BDA_VERSION}-jars.tar.gz" -C "${BDA_PREFIX}/jars"

# Set environment variables.
ENV BDA_HOME "${BDA_PREFIX}"
ENV JAR_FILE="bda-controller-${BDA_VERSION}-jar-with-dependencies.jar"
ENV ENTRYPOINT="gr.ntua.ece.cslab.selis.bda.controller.Entrypoint"
ENV CONF_FILE="${BDA_PREFIX}/conf/bda.properties"

# Add entrypoint
ADD ./entrypoint.sh /
RUN chmod +x /entrypoint.sh

CMD [ "./entrypoint.sh" ]
