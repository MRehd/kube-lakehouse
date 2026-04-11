FROM flink:2.0.0-scala_2.12-java11

RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install apache-flink==2.0.0

RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.0-2.0/flink-sql-connector-kafka-4.0.0-2.0.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.0/1.10.1/iceberg-flink-runtime-2.0-1.10.1.jar

# Enable S3A filesystem plugin (required for s3a:// checkpoint storage)
RUN mkdir -p /opt/flink/plugins/flink-s3-fs-hadoop \
    && cp /opt/flink/opt/flink-s3-fs-hadoop-2.0.0.jar /opt/flink/plugins/flink-s3-fs-hadoop/

COPY jobs/ /opt/flink/jobs/
