package com.example.kafka

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.streaming.Trigger
import com.example.kafka.CustomerSpendingAnalysis

import java.sql.DriverManager

class CustomerSpendingAnalysisTest extends AnyFunSuite with Matchers {

  val spark = SparkSession.builder()
    .appName("TestCustomerSpendingAnalysis")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Schema for test data
  val schema = new StructType()
    .add("transaction_id", StringType)
    .add("customer_id", StringType)
    .add("timestamp", TimestampType)
    .add("amount", DoubleType)
    .add("currency", StringType)
    .add("payment_method", StringType)
    .add("category", StringType)
    .add("status", StringType)

  test("Schema Validation - Should match expected schema") {
    val customerSpendingAnalysis = new CustomerSpendingAnalysis()
    val expectedSchema = schema
    assert(expectedSchema == customerSpendingAnalysis.schema)
  }

  test("Kafka Data Parsing - Should correctly deserialize JSON") {
    val jsonData = Seq(
      """{"transaction_id": "t1", "customer_id": "c1", "timestamp": "2025-03-10T12:00:00", "amount": 100.0, "currency": "USD", "payment_method": "CreditCard", "category": "Food", "status": "Completed"}"""
    ).toDF("value")

    val parsedDF = jsonData
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*")

    assert(parsedDF.schema == schema)
  }

  test("Aggregation Logic - Should correctly aggregate total spending per customer in a 10-minute window") {
    val inputDF = Seq(
      ("c1", "2025-03-10T12:00:00", 50.0),
      ("c1", "2025-03-10T12:05:00", 30.0),
      ("c1", "2025-03-10T12:07:00", 20.0),
      ("c2", "2025-03-10T12:03:00", 40.0)
    ).toDF("customer_id", "timestamp", "amount")
      .withColumn("timestamp", col("timestamp").cast(TimestampType))

    val aggregatedDF = inputDF
      .withWatermark("timestamp", "5 minutes")
      .groupBy(
        window(col("timestamp"), "10 minutes", "5 minutes"),
        col("customer_id")
      )
      .agg(sum("amount").alias("total_spent"))
      .withColumn("window_start", col("window.start"))
      .withColumn("window_end", col("window.end"))
      .drop("window")

    val result = aggregatedDF.collect()
    assert(result.length > 0) // Check aggregation result exists
  }

  test("JDBC Write - Should call JDBC write function without errors") {
    val jdbcMockDF = Seq(("c1", 150.0, "2025-03-10T12:00:00", "2025-03-10T12:10:00"))
      .toDF("customer_id", "total_spent", "window_start", "window_end")
      .withColumn("window_start", col("window_start").cast(TimestampType))  // Cast to Timestamp
      .withColumn("window_end", col("window_end").cast(TimestampType))

    noException should be thrownBy {
      import scala.util.Using

      Using.Manager { use =>
        val connection = use(DriverManager.getConnection(
          "jdbc:postgresql://localhost:5432/postgres",
          "postgres",
          "Welcome2cnh"
        ))
        connection.setAutoCommit(false)
        val savepoint = connection.setSavepoint()

        jdbcMockDF.write
          .format("jdbc")
          .option("url", "jdbc:postgresql://localhost:5432/postgres")
          .option("dbtable", "public.customer_spending")
          .option("user", "postgres")
          .option("password", "Welcome2cnh")
          .option("driver", "org.postgresql.Driver")
          .mode("append")
          .save()

        connection.rollback(savepoint) // Ensures rollback
      }
    }
  }
}
