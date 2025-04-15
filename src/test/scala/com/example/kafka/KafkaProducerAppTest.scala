package com.example.kafka

import org.scalatest.funsuite.AnyFunSuite
import org.apache.kafka.clients.producer.ProducerRecord

class KafkaProducerAppTest extends AnyFunSuite {

  test("generateTransaction should return a valid JSON string") {
    val transaction = KafkaProducerApp.generateTransaction()
    assert(transaction.contains("\"transaction_id\""))
    assert(transaction.contains("\"customer_id\""))
    assert(transaction.contains("\"status\""))
  }

  test("Kafka producer should create a valid message without a key") {
    val topicName = "transactions_topic" // Use the actual topic name
    val value = """{
                  |  "transaction_id": "sample-tx-id",
                  |  "customer_id": 123,
                  |  "merchant_id": 45,
                  |  "timestamp": "2025-03-07T12:34:56Z",
                  |  "amount": 500.75,
                  |  "payment_method": "Credit Card",
                  |  "status": "Success"
                  |}""".stripMargin

    // Creating the Kafka message **without a key**
    val record = new ProducerRecord[String, String](topicName, value)

    // Assertions
    assert(record.topic() == topicName)
    assert(record.value() == value)
    assert(record.key() == null) // Ensure no key is used
  }
}
