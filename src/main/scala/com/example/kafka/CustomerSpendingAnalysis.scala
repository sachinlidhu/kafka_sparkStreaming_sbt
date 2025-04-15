package com.example.kafka

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.expressions.Window

class CustomerSpendingAnalysis {

  val spark = SparkSession.builder()
    .appName("CustomerSpendingAnalysis")
    .master("local[*]") // Run locally with all cores
    .getOrCreate()

  spark.conf.set("spark.sql.streaming.statefulOperator.asyncCheckpoint.enabled", "true")

  spark.conf.set("spark.streaming.backpressure.enabled", "true") // Add this line for backpressure control

  // Corrected Schema: Use TimestampType for "timestamp"
  val schema = new StructType()
    .add("transaction_id", StringType)
    .add("customer_id", StringType)
    .add("merchant_id", IntegerType)
    .add("timestamp", TimestampType)  // FIXED: Changed from StringType to TimestampType
    .add("amount", DoubleType)
    .add("payment_method", StringType)
    .add("status", StringType)

  def readFromKafka(spark: SparkSession, kafkaBootstrapServers: String, topic: String): DataFrame = {

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*")
      .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) // FIXED: Convert string to timestamp
  }

  /*def aggregateTransactionsNew(spark: SparkSession, transactionsDF: DataFrame): DataFrame = {

    // Force Spark to use UTC time zone for all timestamp operations
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    // Ensure timestamp is in UTC to avoid time zone mismatches
    val transactionsWithUTC = transactionsDF
      .withColumn("timestamp", to_utc_timestamp(col("timestamp"), "UTC"))

    val dailyRevenue = transactionsWithUTC
      .withColumn("transaction_date", col("timestamp").cast("date")) // Ensure correct type
      .groupBy("customer_id", "transaction_date")
      .agg(sum("amount").alias("total_spent"))

    dailyRevenue

  }*/

  def aggregateTransactions(spark: SparkSession, transactionsDF: DataFrame): DataFrame = {

    // Force Spark to use UTC time zone for all timestamp operations
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    // Ensure timestamp is in UTC to avoid time zone mismatches
    val transactionsWithUTC = transactionsDF
      .withColumn("timestamp", to_utc_timestamp(col("timestamp"), "UTC"))
      //.dropDuplicates("customer_id", "timestamp")
      .dropDuplicates("transaction_id") // Drop duplicates based on transaction_id

    val windowedDF = transactionsWithUTC
      //.dropDuplicates("customer_id", "timestamp")//
      .withWatermark("timestamp", "5 seconds") // Timestamp column must be of type TimestampType
      .groupBy(
        window(col("timestamp"), "10 minutes", "5 minutes"),
        col("customer_id"),
        col("transaction_id") //can be removed used for testing
      )
      .agg(sum("amount").alias("window_total_spent"))
      .select(
        col("customer_id"),
        col("transaction_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("window_total_spent")
      )

    // Step 2: Aggregate further by customer per day before writing to PostgreSQL
   /* val dailyAggregatedDF = windowedDF
      .withColumn("transaction_date", col("window_start").cast("date"))
      .groupBy("customer_id", "transaction_date")
      .agg(sum("window_total_spent").alias("total_spent"))

    dailyAggregatedDF*/
    windowedDF
  }

  def writeToPostgres1(aggregatedDF: DataFrame, jdbcUrl: String, dbUser: String, dbPassword: String, dbTable: String): Unit = {
    if (aggregatedDF.isStreaming) {
      aggregatedDF.writeStream
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          val dailyAggregatedDF = batchDF
            .dropDuplicates("transaction_id") // Drop duplicates based on transaction_id
            .withColumn("transaction_date", col("window_start").cast("date"))
            .groupBy("customer_id", "transaction_date")
            .agg(sum("window_total_spent").alias("total_spent"))
            .dropDuplicates("customer_id", "transaction_date")
          dailyAggregatedDF.write
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
      val dailyAggregatedDF = aggregatedDF
        .dropDuplicates("transaction_id") // Drop duplicates based on transaction_id
        .withColumn("transaction_date", col("window_start").cast("date"))
        .groupBy("customer_id", "transaction_date")
        .agg(sum("window_total_spent").alias("total_spent"))
        .dropDuplicates("customer_id", "transaction_date")
      dailyAggregatedDF.write
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

  def writeToPostgres(
                       windowedDF: DataFrame,
                       jdbcUrl: String,
                       dbUser: String,
                       dbPassword: String,
                       dbTable: String
                     ): Unit = {

    if (windowedDF.isStreaming) {
      windowedDF.writeStream
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          try {
            val dailyAggregatedDF = batchDF
              .dropDuplicates("transaction_id") // Drop duplicates based on transaction_id
              .withColumn("transaction_date", col("window_start").cast("date"))
              .groupBy("customer_id", "transaction_date")
              .agg(sum("window_total_spent").alias("total_spent"))
              .dropDuplicates("customer_id", "transaction_date") // Avoid overwriting if already written

            dailyAggregatedDF.write
              .format("jdbc")
              .option("url", jdbcUrl)
              .option("dbtable", dbTable)
              .option("user", dbUser)
              .option("password", dbPassword)
              .option("driver", "org.postgresql.Driver")
              .mode("append")
              .save()

            println(s"[Batch $batchId] Successfully written ${dailyAggregatedDF.count()} rows to Postgres.")

          } catch {
            case e: Exception =>
              println(s"[Batch $batchId] Error writing to Postgres: ${e.getMessage}")
          }
        }
        .outputMode("update") // or "append" based on logic
        .start()
    }
  }

}
object CustomerSpendingAnalysis {
  def apply(): CustomerSpendingAnalysis = new CustomerSpendingAnalysis()

  def main(args: Array[String]): Unit = {

    val spendingAnalysis = new CustomerSpendingAnalysis()
    val spark = SparkSession.builder()
      .appName("CustomerBehaviorStreaming")
      .master("local[*]")
      .getOrCreate()

    spark.conf.set("spark.streaming.backpressure.enabled", "true") // Add this line for backpressure control

    val (kafkaBootstrapServers, jdbcUrl, dbUser, dbPassword, topic) =
      if (args.length == 5) {
        // Use parameters from test case if provided
        (args(0), args(1), args(2), args(3), args(4))
      } else {
        // Fallback to config file in actual runs
        val config = ConfigFactory.parseResources("application.conf").resolve()
        (
          config.getString("kafka.bootstrap.servers"),
          config.getString("db.url"),
          config.getString("db.user"),
          config.getString("db.password"),
          "transactions_topic"
        )
      }

    val transactionsDF = spendingAnalysis.readFromKafka(spark, kafkaBootstrapServers, topic)

    val aggregatedDF = spendingAnalysis.aggregateTransactions(spark, transactionsDF)

    spendingAnalysis.writeToPostgres(aggregatedDF, jdbcUrl, dbUser, dbPassword, "public.customer_daily_spending")

    spark.streams.awaitAnyTermination()
    //query.awaitTermination() // Keep the streaming job running
  }
}
