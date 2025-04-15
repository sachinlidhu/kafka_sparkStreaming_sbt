package com.example.kafka

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import com.typesafe.config.ConfigFactory

class CustomerSpendingAnalysisNew1(spark: SparkSession) { //  Inject SparkSession instead of creating a new one

  // Define schema for Kafka messages
  private val schema = new StructType()
    .add("transaction_id", StringType)
    .add("customer_id", StringType)
    .add("merchant_id", IntegerType)
    .add("timestamp", TimestampType)
    .add("amount", DoubleType)
    .add("payment_method", StringType)
    .add("status", StringType)

  /**  Read Data from Kafka Without Starting the Stream */
  def readFromKafka(kafkaBootstrapServers: String, topic: String): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*") //  Extract nested fields
  }

  /**  Perform Aggregation with Proper Watermarking */
  def aggregateTransactions(transactionsDF: DataFrame): DataFrame = {
    spark.conf.set("spark.sql.session.timeZone", "UTC") // Force UTC for consistency

    transactionsDF
      //.withWatermark("timestamp", "5 seconds") //  Proper event-time watermarking
      .groupBy(
        window(col("timestamp"), "10 minutes", "5 minutes"),
        col("customer_id")
      )
      .agg(sum("amount").alias("total_spent"))
      .select(
        col("customer_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_spent")
      )
  }

  /**  Write Data to PostgreSQL */
  def writeToPostgres(aggregatedDF: DataFrame, jdbcUrl: String, dbUser: String, dbPassword: String, dbTable: String): Unit = {
    if (aggregatedDF.isStreaming) {
      aggregatedDF.writeStream
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          batchDF.write
            .format("jdbc")
            .option("url", jdbcUrl)
            .option("dbtable", dbTable)
            .option("user", dbUser)
            .option("password", dbPassword)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        }
        .outputMode("update")
        .start()
    } else {
      aggregatedDF.write
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", dbTable)
        .option("user", dbUser)
        .option("password", dbPassword)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    }
  }
}

object CustomerSpendingAnalysisNew1 {
  def apply(spark: SparkSession): CustomerSpendingAnalysisNew1 = new CustomerSpendingAnalysisNew1(spark)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CustomerBehaviorStreaming")
      .master("local[*]") // Running locally
      .getOrCreate()

    spark.conf.set("spark.streaming.backpressure.enabled", "true") // ✅ Backpressure control

    // Load configurations
    val (kafkaBootstrapServers, jdbcUrl, dbUser, dbPassword, topic) =
      if (args.length == 5) {
        (args(0), args(1), args(2), args(3), args(4))
      } else {
        val config = ConfigFactory.load()
        (
          config.getString("kafka.bootstrap.servers"),
          config.getString("db.url"),
          config.getString("db.user"),
          config.getString("db.password"),
          "transactions_topic"
        )
      }

    val spendingAnalysis = new CustomerSpendingAnalysisNew1(spark) // Pass SparkSession

    val transactionsDF = spendingAnalysis.readFromKafka(kafkaBootstrapServers, topic)

    transactionsDF.writeStream
      .format("console")
      .outputMode("append")
      .start().awaitTermination(20000)

    val aggregatedDF = spendingAnalysis.aggregateTransactions(transactionsDF)

    println(s"******Is Streaming: ${aggregatedDF.isStreaming}")  // Should print true

    aggregatedDF.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"⚡ Processing Batch: $batchId")
        println(s"🔢 Row Count: ${batchDF.count()}")
        batchDF.show(truncate = false)
      }
      .outputMode("update")
      .start()
      .awaitTermination(20000)


    // Check if data is flowing properly before writing to PostgreSQL
  /*  aggregatedDF.writeStream
      .format("console")
      .outputMode("update")
      .start()*/


    spendingAnalysis.writeToPostgres(aggregatedDF, jdbcUrl, dbUser, dbPassword, "public.customer_spending")

    spark.streams.awaitAnyTermination() // Keep streaming job alive
  }
}

