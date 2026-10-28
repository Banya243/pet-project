ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "training"
ThisBuild / version := "0.1.0"
ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-target:jvm-1.8")

lazy val root = (project in file("."))
  .aggregate(sparkJob, flinkJob, kafkaToS3)
  .settings(publish / skip := true)

lazy val sparkJob = (project in file("spark"))
  .settings(
    name := "spark-job",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.8" % Provided,
    Compile / mainClass := Some("lab.spark.EnvironmentCheck"),
    assembly / assemblyJarName := "spark-job.jar",
    assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false)
  )

lazy val flinkJob = (project in file("flink"))
  .settings(
    name := "flink-job",
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-streaming-java" % "1.18.1" % Provided,
      "org.apache.flink" % "flink-clients" % "1.18.1" % Provided
    ),
    Compile / mainClass := Some("lab.flink.EnvironmentCheck"),
    assembly / assemblyJarName := "flink-job.jar"
  )

lazy val kafkaToS3 = (project in file("kafka-to-s3"))
  .settings(
    name := "kafka-to-s3",
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-streaming-java" % "1.18.1" % Provided,
      "org.apache.flink" % "flink-clients" % "1.18.1" % Provided,
      "org.apache.flink" % "flink-connector-files" % "1.18.1" % Provided,
      "org.apache.flink" % "flink-connector-kafka" % "3.2.0-1.18",
      "org.apache.flink" % "flink-avro" % "1.18.1",
      "org.apache.flink" % "flink-parquet" % "1.18.1",
      ("org.apache.parquet" % "parquet-avro" % "1.13.1")
        .excludeAll(ExclusionRule(organization = "org.apache.hadoop")),
      // Parquet needs Hadoop APIs; the S3 filesystem stays in Flink's isolated plugin.
      "org.apache.hadoop" % "hadoop-client-api" % "3.3.4",
      "org.apache.hadoop" % "hadoop-client-runtime" % "3.3.4" % Runtime,
      "org.scalameta" %% "munit" % "1.0.2" % Test
    ),
    Compile / mainClass := Some("lab.kafka.KafkaToS3"),
    Test / fork := true,
    assembly / assemblyJarName := "kafka-to-s3.jar",
    assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _ @ _*) => MergeStrategy.filterDistinctLines
      case "module-info.class" => MergeStrategy.discard
      case path if path.endsWith("/module-info.class") => MergeStrategy.discard
      case path => (assembly / assemblyMergeStrategy).value(path)
    }
  )
