FROM eclipse-temurin:11-jdk-jammy

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/sbt \
    && curl --fail --location --retry 3 \
      https://repo.maven.apache.org/maven2/org/scala-sbt/sbt-launch/1.10.11/sbt-launch-1.10.11.jar \
      --output /opt/sbt/sbt-launch.jar

COPY repositories /opt/sbt/repositories
ENV COURSIER_REPOSITORIES=https://repo.maven.apache.org/maven2
WORKDIR /workspace/scala
ENTRYPOINT ["java", "-Xms256m", "-Xmx1536m", "-XX:ActiveProcessorCount=2", "-Dsbt.task.cpus=2", "-Dsbt.supershell=false", "-Dsbt.color=false", "-Dsbt.override.build.repos=true", "-Dsbt.repository.config=/opt/sbt/repositories", "-jar", "/opt/sbt/sbt-launch.jar"]
CMD ["shell"]
