package com.example.kafka

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.{KafkaContainer, PostgreSQLContainer}
import org.testcontainers.utility.DockerImageName
import org.apache.spark.sql.functions._

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType, TimestampType}

case class Transaction(
                        transaction_id: String,
                        customer_id: String,
                        merchant_id: Long,
                        timestamp: String,
                        amount: Double,
                        payment_method: String,
                        status: String
                      )

class CustomerSpendingIntegrationTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {
  val customerSpendingAnalysis = CustomerSpendingAnalysis()

  val kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.0")).withEmbeddedZookeeper()
  val postgresContainer = new PostgreSQLContainer(DockerImageName.parse("postgres:14"))

  kafkaContainer.start()
  postgresContainer.start()
  Thread.sleep(5000) // Wait for containers to start

  val spark = SparkSession.builder()
    .appName("CustomerSpendingIntegrationTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val kafkaBootstrapServers = kafkaContainer.getBootstrapServers
  val jdbcUrl = postgresContainer.getJdbcUrl
  val user = postgresContainer.getUsername
  val password = postgresContainer.getPassword
  val testTable = "public.test_customer_spending1"  // Using a test table

  // Setup: Truncate test table before/after tests
  override def beforeEach(): Unit = {
    val connection = java.sql.DriverManager.getConnection(jdbcUrl, user, password)
    val statement = connection.createStatement()

    // Ensure the table exists
    statement.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS $testTable (
         |  customer_id VARCHAR(50),
         |  transaction_date DATE NOT NULL,
         |  total_spent DOUBLE PRECISION,
         |  PRIMARY KEY (customer_id, transaction_date)
         |);
         |""".stripMargin)
      /*
      statement.execute(
        s
         |CREATE TABLE IF NOT EXISTS $testTable (
         |  customer_id VARCHAR(50),
         |  window_start TIMESTAMP,
         |  window_end TIMESTAMP,
         |  total_spent DOUBLE PRECISION
         |);
         |""".stripMargin
         )
*/
    // Clean the table before running tests
    statement.execute(s"TRUNCATE TABLE $testTable;")

    statement.close()
    connection.close()
    // ---- CREATE KAFKA TOPIC ----
    import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
    import scala.jdk.CollectionConverters._ // <-- needed for .asJava

    val props = new Properties()
    props.put("bootstrap.servers", kafkaBootstrapServers)

    val adminClient = AdminClient.create(props)
    val topic = "transactions_topic"

    val newTopic = new NewTopic(topic, 1, 1.toShort) // 1 partition, 1 replication factor
    //adminClient.createTopics(java.util.Collections.singletonList(newTopic))
    adminClient.createTopics(List(newTopic).asJava).all().get() // Wait for creation
    adminClient.close()
  }


  override def afterEach(): Unit = {
    val connection = java.sql.DriverManager.getConnection(jdbcUrl, user, password)
    val statement = connection.createStatement()
    statement.execute(s"TRUNCATE TABLE $testTable;") // Clean table after test
    statement.close()
    connection.close()
  }

  def produceToKafka(topic: String, messages: Seq[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaBootstrapServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    messages.foreach { message =>
      producer.send(new ProducerRecord[String, String](topic, message))
    }
    producer.close()
  }

  test("Kafka Integration - Should consume messages from Kafka topic") {
    val topic = "transactions_topic_new"

    val testMessages = Seq(
      """{"transaction_id": "t1", "customer_id": 1, "merchant_id": 10, "timestamp": "2025-03-10T12:01:00Z", "amount": 100.0, "payment_method": "Credit Card", "status": "Success"}""",
      """{"transaction_id": "t2", "customer_id": 1, "merchant_id": 20, "timestamp": "2025-03-10T12:05:00Z", "amount": 150.0, "payment_method": "UPI", "status": "Pending"}"""
    )
    produceToKafka(topic, testMessages)

    val df = customerSpendingAnalysis.readFromKafka(spark, kafkaBootstrapServers, topic)

    assert(df.isStreaming)

    // Write the streaming DataFrame to an in-memory table
    val query = df.writeStream
      .format("memory") // Allows querying the table for testing
      .queryName("test_kafka_data")
      .outputMode("append")
      .start()

    // Wait for a fixed time to ensure the stream processes all available data (2 messages)
    query.processAllAvailable() // Ensure all messages are processed

    // Use a timeout to avoid indefinite waiting
    query.awaitTermination(5000) // Wait for up to 5 seconds

    // Query the in-memory table
    val resultCount = spark.sql("SELECT COUNT(*) FROM test_kafka_data").collect().head.getLong(0)

    // Assert that some records were processed
    assert(resultCount > 0)

    query.stop() // Stop the stream
  }


  test("Aggregation - Should correctly group transactions into 10-minute windows") {

    val schema = new StructType()
      .add("transaction_id", StringType)
      .add("customer_id", StringType)
      .add("merchant_id", IntegerType)
      .add("timestamp", TimestampType)  // FIXED: Changed from StringType to TimestampType
      .add("amount", DoubleType)
      .add("payment_method", StringType)
      .add("status", StringType)

    val sampleData = Seq(
      """{"transaction_id": "t1", "customer_id": 1, "merchant_id": 10, "timestamp": "2025-03-10T12:01:00Z", "amount": 100.0, "payment_method": "Credit Card", "status": "Success"}""",
      """{"transaction_id": "t2", "customer_id": 1, "merchant_id": 10, "timestamp": "2025-03-10T12:05:00Z", "amount": 150.0, "payment_method": "UPI", "status": "Pending"}""",
      """{"transaction_id": "t3", "customer_id": 1, "merchant_id": 10, "timestamp": "2025-03-10T12:11:00Z", "amount": 200.0, "payment_method": "Net Banking", "status": "Success"}"""
    ).toDF("value")
      .withColumn("data", from_json(col("value"), schema))
      .selectExpr("data.*")  //it is now struct type-- value is string type
      .withColumn("timestamp", to_timestamp(col("timestamp")))

    println("************__aggregateDF__started__*************************************")
    val aggregatedDF = customerSpendingAnalysis.aggregateTransactions(spark, sampleData)
    aggregatedDF.show(truncate = false)
    println("************__aggregateDF__completed__*************************************")

    val expectedData = Seq(
      ("1","t3", "2025-03-10 12:05:00", "2025-03-10 12:15:00", 200.0),  // t1 + t2
      ("1","t3", "2025-03-10 12:10:00", "2025-03-10 12:20:00", 200.0),  // t2 + t3
      ("1","t2", "2025-03-10 12:00:00", "2025-03-10 12:10:00", 150.0),  // t1 only (earlier window)
      ("1","t2", "2025-03-10 12:05:00", "2025-03-10 12:15:00", 150.0),
      ("1","t1", "2025-03-10 11:55:00", "2025-03-10 12:05:00", 100.0),
      ("1","t1", "2025-03-10 12:00:00", "2025-03-10 12:10:00", 100.0)// t3 only
    ).toDF("customer_id","transaction_id", "window_start", "window_end", "window_total_spent")
      .withColumn("window_start", to_timestamp(col("window_start")))
      .withColumn("window_end", to_timestamp(col("window_end")))

    println("*************expected data************************************")
    println("*************************************************")
    expectedData.show(truncate = false)
    assert(aggregatedDF.count() == expectedData.count())

    println("*************************************************")
    println("*************************************************")
    //aggregatedDF.show(truncate = false)
    //expectedData.show(truncate = false)

    //val actualData = aggregatedDF.collect().map(row => (row.getInt(0), row.getTimestamp(1), row.getTimestamp(2), row.getDouble(3))).toSet
    val actualData = aggregatedDF.collect().map(row => (
      row.getAs[String]("customer_id"),  // Ensure customer_id is treated as a String
      row.getAs[String]("transaction_id"),
      row.getAs[java.sql.Timestamp]("window_start"),
      row.getAs[java.sql.Timestamp]("window_end"),
      row.getAs[Double]("window_total_spent")
    )).toSet

    //val expectedDataSet = expectedData.collect().map(row => (row.getInt(0), row.getTimestamp(1), row.getTimestamp(2), row.getDouble(3))).toSet

    val expectedDataSet = expectedData.collect().map(row => (
      row.getAs[String]("customer_id"),  // Fix: Get customer_id as String instead of Int
      row.getAs[String]("transaction_id"),
      row.getAs[java.sql.Timestamp]("window_start"),
      row.getAs[java.sql.Timestamp]("window_end"),
      row.getAs[Double]("window_total_spent")
    )).toSet

    assert(actualData == expectedDataSet)

    val actualDF = aggregatedDF.select("customer_id", "transaction_id", "window_start", "window_end", "window_total_spent")
    val expectedDF = expectedData.select("customer_id", "transaction_id", "window_start", "window_end", "window_total_spent")

    assert(actualDF.except(expectedDF).isEmpty)
    assert(expectedDF.except(actualDF).isEmpty)
  }
/*  test("Aggregation - Should correctly group transactions into 10-minute windows") {

    val schema = new StructType()
      .add("transaction_id", StringType)
      .add("customer_id", StringType)
      .add("merchant_id", IntegerType)
      .add("timestamp", TimestampType)  // FIXED: Changed from StringType to TimestampType
      .add("amount", DoubleType)
      .add("payment_method", StringType)
      .add("status", StringType)

    val sampleData = Seq(
      """{"transaction_id": "t1", "customer_id": 1, "merchant_id": 10, "timestamp": "2025-03-10T12:01:00Z", "amount": 100.0, "payment_method": "Credit Card", "status": "Success"}""",
      """{"transaction_id": "t2", "customer_id": 1, "merchant_id": 10, "timestamp": "2025-03-10T12:05:00Z", "amount": 150.0, "payment_method": "UPI", "status": "Pending"}""",
      """{"transaction_id": "t3", "customer_id": 1, "merchant_id": 10, "timestamp": "2025-03-10T12:11:00Z", "amount": 200.0, "payment_method": "Net Banking", "status": "Success"}"""
    ).toDF("value")
      .withColumn("data", from_json(col("value"), schema))
      .selectExpr("data.*")  //it is now struct type-- value is string type
      .withColumn("timestamp", to_timestamp(col("timestamp")))

    println("************__aggregateDF__started__*************************************")
    val aggregatedDF = customerSpendingAnalysis.aggregateTransactions(spark, sampleData)
    aggregatedDF.show(truncate = false)
    println("************__aggregateDF__completed__*************************************")

    val expectedData = Seq(
      ("1", "2025-03-10 12:00:00", "2025-03-10 12:10:00", 250.0),  // t1 + t2
      ("1", "2025-03-10 12:05:00", "2025-03-10 12:15:00", 350.0),  // t2 + t3
      ("1", "2025-03-10 11:55:00", "2025-03-10 12:05:00", 100.0),  // t1 only (earlier window)
      ("1", "2025-03-10 12:10:00", "2025-03-10 12:20:00", 200.0)   // t3 only
    ).toDF("customer_id", "window_start", "window_end", "total_spent")
      .withColumn("window_start", to_timestamp(col("window_start")))
      .withColumn("window_end", to_timestamp(col("window_end")))

    println("*************expected data************************************")
    println("*************************************************")
    expectedData.show(truncate = false)
    assert(aggregatedDF.count() == expectedData.count())

    println("*************************************************")
    println("*************************************************")
    //aggregatedDF.show(truncate = false)
    //expectedData.show(truncate = false)

    //val actualData = aggregatedDF.collect().map(row => (row.getInt(0), row.getTimestamp(1), row.getTimestamp(2), row.getDouble(3))).toSet
    val actualData = aggregatedDF.collect().map(row => (
      row.getAs[String]("customer_id"),  // Ensure customer_id is treated as a String
      row.getAs[java.sql.Timestamp]("window_start"),
      row.getAs[java.sql.Timestamp]("window_end"),
      row.getAs[Double]("total_spent")
    )).toSet

    //val expectedDataSet = expectedData.collect().map(row => (row.getInt(0), row.getTimestamp(1), row.getTimestamp(2), row.getDouble(3))).toSet

    val expectedDataSet = expectedData.collect().map(row => (
      row.getAs[String]("customer_id"),  // Fix: Get customer_id as String instead of Int
      row.getAs[java.sql.Timestamp]("window_start"),
      row.getAs[java.sql.Timestamp]("window_end"),
      row.getAs[Double]("total_spent")
    )).toSet

    assert(actualData == expectedDataSet)

    val actualDF = aggregatedDF.select("customer_id", "window_start", "window_end", "total_spent")
    val expectedDF = expectedData.select("customer_id", "window_start", "window_end", "total_spent")

    assert(actualDF.except(expectedDF).isEmpty)
    assert(expectedDF.except(actualDF).isEmpty)
  }*/
/*
  test("PostgreSQL Integration - Should write and read aggregated data correctly") {
    val sampleData = Seq(
      ("c1", "2025-03-10T12:00:00", "2025-03-10T12:10:00", 150.0)
    ).toDF("customer_id", "window_start", "window_end", "total_spent")
      .withColumn("window_start", to_timestamp(col("window_start")))
      .withColumn("window_end", to_timestamp(col("window_end")))

    customerSpendingAnalysis.writeToPostgres(sampleData, jdbcUrl, user, password, testTable)

    val resultDF = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", testTable)
      .option("user", user)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .load()

    val expectedData = Seq(
      ("c1", "2025-03-10 12:00:00", "2025-03-10 12:10:00", 150.0)
    ).toDF("customer_id", "window_start", "window_end", "total_spent")
      .withColumn("window_start", to_timestamp(col("window_start")))
      .withColumn("window_end", to_timestamp(col("window_end")))

    assert(resultDF.count() == expectedData.count())

    val actualData = resultDF.collect().map(row => (row.getString(0), row.getTimestamp(1), row.getTimestamp(2), row.getDouble(3))).toSet
    val expectedDataSet = expectedData.collect().map(row => (row.getString(0), row.getTimestamp(1), row.getTimestamp(2), row.getDouble(3))).toSet

    assert(actualData == expectedDataSet)
  }*/


  test("PostgreSQL Integration - Should write and read aggregated data correctly") {
    val sampleData = Seq(
      ("c1","t1", "2025-03-10T12:00:00", "2025-03-10T12:10:00", 150.0)
    ).toDF("customer_id", "transaction_id","window_start", "window_end", "window_total_spent")
      .withColumn("window_start", to_timestamp(col("window_start")))
      .withColumn("window_end", to_timestamp(col("window_end")))

    customerSpendingAnalysis.writeToPostgres(sampleData, jdbcUrl, user, password, testTable)

    val resultDF = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", testTable)
      .option("user", user)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .load()

    val expectedData = Seq(
      ("c1", java.sql.Date.valueOf("2025-03-10"), 150.0)
    ).toDF("customer_id", "transaction_date", "total_spent")

    val actual = resultDF
      .withColumn("transaction_date", to_date(col("transaction_date")))
      .select("customer_id", "transaction_date", "total_spent")
      .orderBy("customer_id")

    val expected = expectedData
      .select("customer_id", "transaction_date", "total_spent")
      .orderBy("customer_id")

    println("Actual:")
    actual.show(false)

    println("Expected:")
    expected.show(false)
 /*   println("Actual:")
    actual.foreach(println)

    println("Expected:")
    expected.foreach(println)*/

    assert(actual.collect().sameElements(expected.collect()))

    /*val sortedResult = resultDF.orderBy("customer_id").collect()
    val sortedExpected = expectedData.orderBy("customer_id").collect()*/


    //assert(sortedResult.sameElements(sortedExpected))
  }

  test("aggregateTransactions + daily aggregation logic should compute expected result") {

    import org.apache.spark.sql.functions._
    //import scala.reflect.runtime.universe._

    val spark = SparkSession.builder()
      .appName("Test")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Step 1: Sample input
    val sampleData = Seq(
      """{"transaction_id": "t1", "customer_id": "1", "merchant_id": 10, "timestamp": "2025-03-10T12:01:00Z", "amount": 100.0, "payment_method": "Credit Card", "status": "Success"}""",
      """{"transaction_id": "t2", "customer_id": "1", "merchant_id": 10, "timestamp": "2025-03-10T12:05:00Z", "amount": 150.0, "payment_method": "UPI", "status": "Pending"}""",
      """{"transaction_id": "t3", "customer_id": "1", "merchant_id": 10, "timestamp": "2025-03-10T12:11:00Z", "amount": 200.0, "payment_method": "Net Banking", "status": "Success"}""",
      """{"transaction_id": "t3", "customer_id": "1", "merchant_id": 10, "timestamp": "2025-03-10T12:11:00Z", "amount": 200.0, "payment_method": "Net Banking", "status": "Success"}""",
      """{"transaction_id": "t9", "customer_id": "1", "merchant_id": 10, "timestamp": "2025-03-11T12:11:00Z", "amount": 2000.0, "payment_method": "Net Banking", "status": "Success"}""",
      """{"transaction_id": "t4", "customer_id": "2", "merchant_id": 11, "timestamp": "2025-03-11T09:00:00Z", "amount": 300.0, "payment_method": "Credit Card", "status": "Success"}""",
      """{"transaction_id": "t5", "customer_id": "2", "merchant_id": 11, "timestamp": "2025-03-11T09:05:00Z", "amount": 50.0, "payment_method": "UPI", "status": "Failed"}""",
      """{"transaction_id": "t6", "customer_id": "3", "merchant_id": 12, "timestamp": "2025-03-12T10:00:00Z", "amount": 500.0, "payment_method": "Credit Card", "status": "Success"}""",
      """{"transaction_id": "t7", "customer_id": "3", "merchant_id": 12, "timestamp": "2025-03-12T10:03:00Z", "amount": 100.0, "payment_method": "Net Banking", "status": "Success"}""",
      """{"transaction_id": "t8", "customer_id": "3", "merchant_id": 12, "timestamp": "2025-03-12T10:06:00Z", "amount": 200.0, "payment_method": "UPI", "status": "Success"}"""
    ).toDF("value")

    // Schema for decoding
    val schema = new StructType()
      .add("transaction_id", StringType)
      .add("customer_id", StringType)
      .add("merchant_id", IntegerType)
      .add("timestamp", TimestampType)
      .add("amount", DoubleType)
      .add("payment_method", StringType)
      .add("status", StringType)

    val parsedDF = sampleData
      .withColumn("data", from_json(col("value"), schema))
      .selectExpr("data.*")
      .withColumn("timestamp", to_timestamp(col("timestamp")))

    // Step 2: Run aggregateTransactions (simulates the streaming windowed part)
    val windowedDF = customerSpendingAnalysis.aggregateTransactions(spark, parsedDF)

    windowedDF.show(false)
    val jdbcUrl = postgresContainer.getJdbcUrl
    val user = postgresContainer.getUsername
    val password = postgresContainer.getPassword

    println(s"JDBC URL: $jdbcUrl")
    println(s"User: $user")
    println(s"Password: $password")

    // Step 3: Simulate foreachBatch aggregation logic
    val dailyAggregatedDF = windowedDF
      .dropDuplicates("transaction_id") // Simulate dedup logic
      .withColumn("transaction_date", col("window_start").cast("date"))
      .groupBy("customer_id", "transaction_date")
      .agg(sum("window_total_spent").alias("total_spent"))
      .dropDuplicates("customer_id", "transaction_date") // simulate dedup logic

    // Step 4: Expected result
    val expectedData = Seq(
      ("1", java.sql.Date.valueOf("2025-03-11"), 2000.0),
      ("1", java.sql.Date.valueOf("2025-03-10"), 450.0),
      ("2", java.sql.Date.valueOf("2025-03-11"), 350.0),
      ("3", java.sql.Date.valueOf("2025-03-12"), 800.0)
    ).toDF("customer_id", "transaction_date", "total_spent")

    // Step 5: Validate result
    val sortedResult = dailyAggregatedDF.orderBy("customer_id")//.collect()
    val sortedExpected = expectedData.orderBy("customer_id")//.collect()

    println("Actual:")
    sortedResult.show(false)
   // sortedResult.foreach(println)

    println("Expected:")
    sortedExpected.show(false)
    //sortedExpected.foreach(println)

    assert(sortedResult == sortedExpected)
    //assert(sortedResult.sameElements(sortedExpected))
  }

  // End-to-End Integration Test
  test("End-to-End Integration Test - Kafka to PostgreSQL") {

    // Step 1: Produce test messages to Kafka
    val topic = "transactions_topic"
    val testMessages = Seq(
      """{"transaction_id": "t1", "customer_id": 1, "merchant_id": 10, "timestamp": "2025-03-10T12:01:00Z", "amount": 100.0, "payment_method": "Credit Card", "status": "Success"}"""
    )
    produceToKafka(topic, testMessages)

    Thread.sleep(5000)
    // Step 2: Call the main function of your Spark application
    CustomerSpendingAnalysis.main(Array(kafkaBootstrapServers, jdbcUrl, user, password, testTable))


    // Wait to ensure Spark finishes processing (optional: replace with a better synchronization mechanism)
    Thread.sleep(5000)

    // Step 3: Validate PostgreSQL contains expected results
    val resultDF = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", testTable)
      .option("user", user)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .load()

    assert(resultDF.count() > 0) // Ensure data is written
  }

}
