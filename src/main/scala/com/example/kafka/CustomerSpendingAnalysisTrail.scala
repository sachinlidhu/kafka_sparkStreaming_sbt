package com.example.kafka

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object CustomerSpendingAnalysisTrail {

  // Define Logger
  //private val logger1 = Logger.getLogger(this.getClass)
  private val logger1 = Logger.getLogger(CustomerSpendingAnalysis.getClass)

  // Define Schema
  val schema = new StructType()
    .add("transaction_id", StringType)
    .add("customer_id", StringType)
    .add("timestamp", TimestampType)
    .add("amount", DoubleType)
    .add("currency", StringType)
    .add("payment_method", StringType)
    .add("category", StringType)
    .add("status", StringType)

  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(CustomerSpendingAnalysis.getClass)

    //println("***********************************************************************************************************")
    logger.info("***********************************************************************************************************")
    val spark = SparkSession.builder()
      .appName("CustomerBehaviorStreaming")
      .master("local[*]") // Running locally
      .getOrCreate()


    // Read from Kafka
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "transactions_topic")
      .option("startingOffsets", "earliest")
      .load()

    System.setProperty("org.postgresql.forceBinary", "true")

    // Deserialize JSON
    val transactionsDF = kafkaStream
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*")

    // Apply a 10-minute sliding window for aggregation
    val aggregatedDF = transactionsDF
      .withWatermark("timestamp", "5 minutes") // Allow 5 min late arrival
      .groupBy(
        window(col("timestamp"), "10 minutes", "5 minutes"), // Sliding window (10 min, every 5 min)
        col("customer_id")
      )
      .agg(sum("amount").alias("total_spent"))
      .withColumn("window_start", col("window.start")) // Extract window start
      .withColumn("window_end", col("window.end")) // Extract window end
      .drop("window")

    import com.typesafe.config.ConfigFactory

    // Load config
    //val config = ConfigFactory.parseResources("application.conf").resolve()
    val config = ConfigFactory.load()
    val jdbcUrl = config.getString("db.url")
    val dbUser = config.getString("db.user")
    val dbPassword = config.getString("db.password")
    val dbDriver = config.getString("db.driver")
    val dbTable = "public.customer_spending"


    // Write results to PostgreSQL
    val query = aggregatedDF.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        batchDF.write
          .format("jdbc")
          .option("url", jdbcUrl)
          .option("dbtable", dbTable)
          .option("user", dbUser)
          .option("password", dbPassword)
          .option("driver", dbDriver)
          .mode("append")
          .save()

      }
      .outputMode("update") // Since we're aggregating, use "update" mode
      .start()

    query.awaitTermination()
  }
}
