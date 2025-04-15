package com.example.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.log4j.Logger

import java.util.{Properties, UUID}
import scala.util.Random
import java.time.Instant

object KafkaProducerApp extends App {

  // Define Logger
  private val logger = Logger.getLogger(this.getClass)

  logger.info("Starting Kafka Producer...")

  // Kafka properties
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put("enable.idempotence", "false")

  // Create Kafka Producer
  val producer = new KafkaProducer[String, String](props)
  val topic = "transactions_topic"

  // Function to generate a random transaction
  def generateTransaction(): String = {
    val transactionId = UUID.randomUUID().toString
    val customerId = Random.nextInt(1000) + 1  // Customer ID between 1 and 1000
    val amount = BigDecimal(Random.nextDouble() * 1000).setScale(2, BigDecimal.RoundingMode.HALF_UP) // Amount between 0 and 1000
    val timestamp = Instant.now.toString
    val paymentMethod = List("Credit Card", "Debit Card", "PayPal", "UPI", "Net Banking").apply(Random.nextInt(5))
    val merchantId = Random.nextInt(500) + 1  // Merchant ID between 1 and 500
    val transactionStatus = List("Success", "Pending", "Failed").apply(Random.nextInt(3))

    // Creating JSON format manually
    s"""{
       |  "transaction_id": "$transactionId",
       |  "customer_id": $customerId,
       |  "merchant_id": $merchantId,
       |  "timestamp": "$timestamp",
       |  "amount": $amount,
       |  "payment_method": "$paymentMethod",
       |  "status": "$transactionStatus"
       |}""".stripMargin
  }

  // Produce messages continuously
  while (true) {
    val message = generateTransaction()
    val record = new ProducerRecord[String, String](topic, message)
    producer.send(record)

    //logger.info(s"Sent: $message")
    println(s"Sent: $message")
    Thread.sleep(2000) // Send a new transaction every 2 seconds
  }

  // Close the producer (this won't execute as the loop runs infinitely)
  producer.close()
}
