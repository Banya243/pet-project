FROM apache/spark:3.5.8-scala2.12-java11-python3-ubuntu

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openssh-server curl \
    && rm -rf /var/lib/apt/lists/* \
    && echo 'spark:spark' | chpasswd \
    && usermod --shell /bin/bash --home /home/spark spark \
    && mkdir -p /run/sshd /home/spark \
    && chown spark:spark /home/spark \
    && curl --fail --location --retry 3 \
      https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      --output /opt/spark/jars/hadoop-aws-3.3.4.jar \
    && curl --fail --location --retry 3 \
      https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      --output /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    && curl --fail --location --retry 3 \
      https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar \
      --output /opt/spark/jars/postgresql-42.7.5.jar

COPY sshd_config /etc/ssh/sshd_config
COPY --chmod=755 entrypoint.sh /usr/local/bin/lab-spark-entrypoint
COPY --chmod=755 submit-spark.sh /usr/local/bin/submit-spark

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/lab-spark-entrypoint"]
CMD ["gateway"]
