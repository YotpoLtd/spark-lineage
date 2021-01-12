package org.chojin.spark.lineage.reporter

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.chojin.spark.lineage.report.Report

case class KafkaReporter(bootstrapServerUrl: String,
                         topic: String,
                         compression: Option[String]) extends Reporter {
  def this(props: Map[String, String]) = this(
    bootstrapServerUrl=props("bootstrapServerUrl"),
    topic=props("topic"),
    compression=props.get("compression"))

  private lazy val producer = {
    val props = new Properties()
    props.put("bootstrap.servers", this.bootstrapServerUrl)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[Null, String](props)
  }

  override def report(report: Report): Unit = {
    val payload = report.toJson()

    val record = new ProducerRecord(this.topic,null, payload)
    producer.send(record)
  }
}

