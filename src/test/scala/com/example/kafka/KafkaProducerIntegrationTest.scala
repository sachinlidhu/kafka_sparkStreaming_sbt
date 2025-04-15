package com.example.kafka

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import java.util.Properties

class KafkaProducerIntegrationTest extends AnyFunSuite with BeforeAndAfterAll {

  var producer: KafkaProducer[String, String] = _

  override def beforeAll(): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    producer = new KafkaProducer[String, String](props)
  }

  test("Kafka Producer should send message successfully") {
    val record = new ProducerRecord[String, String]("transactions_topic", """{"transaction": "test"}""") // No key
    val metadata = producer.send(record).get()  // Waits for Kafka response

    assert(metadata.topic() == "transactions_topic")
  }

  override def afterAll(): Unit = {
    producer.close()
  }
}
