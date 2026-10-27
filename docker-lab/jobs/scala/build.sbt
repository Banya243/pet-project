ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "training"
ThisBuild / version := "0.1.0"
ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-target:jvm-1.8")

lazy val root = (project in file("."))
  .aggregate(sparkJob, flinkJob)
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
