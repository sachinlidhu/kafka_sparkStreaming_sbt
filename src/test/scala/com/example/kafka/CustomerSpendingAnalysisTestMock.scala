package com.example.kafka

import com.example.kafka.CustomerSpendingAnalysis

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.mockito.Mockito.{doNothing, doReturn, spy, times, verify}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.ArgumentMatchers.any

class CustomerSpendingAnalysisTestMock extends AnyFunSuite with MockitoSugar {

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("CustomerSpendingAnalysisTest")
    .getOrCreate()

  import spark.implicits._

  test("End-to-End Integration Test - Mocked Kafka to PostgreSQL") {
    // Step 1: Create a test DataFrame (Mock Kafka Data)
    //alternative is -- val df = spark.read.option("header", true).csv("data/sample.csv")
    val mockKafkaDF: DataFrame = Seq(
      ("t1", "1", 10, "2025-03-10 12:01:00", 100.0, "Credit Card", "Success"),
      ("t2", "2", 20, "2025-03-10 12:05:00", 150.0, "Debit Card", "Failed")
    ).toDF("transaction_id", "customer_id", "merchant_id", "timestamp", "amount", "payment_method", "status")
      .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

    //import com.example.kafka.CustomerSpendingAnalysis
    // Step 2: Use a Spy instead of Mock for Singleton Object
    val customerSpendingAnalysis = new CustomerSpendingAnalysis()
    val mockCustomerSpending = spy(customerSpendingAnalysis)

   /** We're making a copy of the actual class, but with "spy powers" — we can override certain functions (like readFromKafka) and say “don’t do the real thing, just return my dummy data”.

    Alternative:

      Use a full mock instead of a spy (for traits/classes).


    val mock = mock[CustomerSpendingAnalysis]
    Pros: Lighter and faster
    Cons: You need to define all the methods if you mock the full class*/

    // Mock `readFromKafka` using doReturn instead of when(...).thenReturn(...)
    doReturn(mockKafkaDF).when(mockCustomerSpending).readFromKafka(any(), any(), any())

    // Step 3: Call the aggregation function
    val aggregatedDF = mockCustomerSpending.aggregateTransactions(spark, mockKafkaDF)

    // Step 4: Validate the transformation logic
    assert(aggregatedDF.count() > 0) // Ensure data exists

    val result = aggregatedDF.select("customer_id", "total_spent").collect()
    //assert(result.length == 2) // Ensure 2 customers exist
    val uniqueCustomers = result.map(_.getAs[String]("customer_id"))
    assert(uniqueCustomers.length == 4)
    assert(result.map(_.getAs[Double]("total_spent")).sum == 500.0) // Check total spent

    // Step 5: Mock PostgreSQL write function (optional)
    val mockWrite = spy(customerSpendingAnalysis)
    doNothing().when(mockWrite).writeToPostgres(any(), any(), any(), any(), any())

    // Call the write function (mocked)
    mockWrite.writeToPostgres(aggregatedDF, "jdbc:postgresql://localhost/test", "user", "pass", "test_table")

    // Verify it was called exactly once
    verify(mockWrite, times(1)).writeToPostgres(any(), any(), any(), any(), any())
  }
}
