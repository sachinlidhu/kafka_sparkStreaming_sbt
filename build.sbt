ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "KafkaToPostgresStreaming",
    version := "0.1.0",

    libraryDependencies ++= Seq(
      // Apache Spark Dependencies
      "org.apache.spark" %% "spark-core" % "3.5.0",
      "org.apache.spark" %% "spark-sql" % "3.5.0",
      "org.apache.spark" %% "spark-streaming" % "3.5.0",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",

      // Kafka Dependencies
      "org.apache.kafka" %% "kafka" % "3.7.0",
      "org.apache.kafka" % "kafka-clients" % "3.7.0",

      // Kafka & Schema Registry Dependencies
      "io.confluent" % "kafka-schema-registry-client" % "7.4.0",
      "io.confluent" % "kafka-avro-serializer" % "7.4.0",

      // PostgreSQL JDBC Driver
      "org.postgresql" % "postgresql" % "42.5.1",

      // Logging
      "ch.qos.logback" % "logback-classic" % "1.4.14",

      // JSON Parsing
      "com.typesafe.play" %% "play-json" % "2.9.4",

      // ScalaTest for Unit Testing
      "org.scalatest" %% "scalatest" % "3.2.16" % Test,
      "org.scalatestplus" %% "scalacheck-1-15" % "3.2.10.0" % Test,  // Correct version
      "org.testcontainers" % "testcontainers" % "1.19.3" % Test,
      "org.testcontainers" % "postgresql" % "1.19.3" % Test,
      "org.testcontainers" % "kafka" % "1.19.3" % Test,
      "org.scalatestplus" %% "mockito-4-11" % "3.2.16.0" % Test,

      // Embedded Kafka for Integration Tests
      "io.github.embeddedkafka" %% "embedded-kafka" % "3.4.0" % Test,

      // typesafe config dependency
      "com.typesafe" % "config" % "1.4.2",

      // Logging Dependencies
      "ch.qos.logback" % "logback-classic" % "1.2.11",
      "org.slf4j" % "slf4j-api" % "1.7.32",

      // Apache Spark Dependencies (Exclude Log4j-to-SLF4J)
      "org.apache.spark" %% "spark-core" % "3.5.0" exclude("org.apache.logging.log4j", "log4j-to-slf4j"),
      "org.apache.spark" %% "spark-sql" % "3.5.0" exclude("org.apache.logging.log4j", "log4j-to-slf4j"),
      "org.apache.spark" %% "spark-streaming" % "3.5.0" exclude("org.apache.logging.log4j", "log4j-to-slf4j"),
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0" exclude("org.apache.logging.log4j", "log4j-to-slf4j")

    ),
    // Ensure application.conf is included in the resources directory
    Compile / unmanagedResourceDirectories += baseDirectory.value / "src/main/resources",


    // Set up alternative resolvers in case of dependency issues
    resolvers ++= Seq(
      "Maven Central" at "https://repo1.maven.org/maven2/",
      "Confluent Repository" at "https://packages.confluent.io/maven/",
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
    )
  )
