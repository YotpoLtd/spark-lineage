package org.chojin.spark.lineage.reporter

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.log4j.{Level, Logger}
import org.chojin.spark.lineage.inputs.{Field, HiveInput, How}
import org.chojin.spark.lineage.outputs.FsOutput
import org.chojin.spark.lineage.report.{Metadata, Report}
import org.chojin.spark.lineage.reporter.MockReport.report
import org.junit.runner.RunWith
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.junit4.PowerMockRunner
import org.scalatest.mockito.MockitoSugar.mock
import org.scalatest.{BeforeAndAfterEach, FunSuite}

@RunWith(classOf[PowerMockRunner])
@PrepareForTest(Array(classOf[KafkaReporter]))
class KafkaReporterTest extends FunSuite with BeforeAndAfterEach {

  test("Attempting to mock new") {
    val kafkaProducer = mock[KafkaProducer[Null, String]]

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    //PowerMockito.whenNew(classOf[KafkaProducer[Null, String]]).withArguments(props).thenReturn(kafkaProducer)
    PowerMockito.whenNew(classOf[KafkaProducer[Null, String]]).withParameterTypes(classOf[Properties]).withArguments(props).thenReturn(kafkaProducer)
    PowerMockito.whenNew(classOf[KafkaProducer[Null, String]]).withParameterTypes(classOf[Properties]).withArguments(classOf[Properties]).thenReturn(kafkaProducer)
    PowerMockito.whenNew(classOf[KafkaProducer[Null, String]]).withAnyArguments().thenReturn(kafkaProducer)

    var shouldBeMockedProducer = new KafkaProducer[Null, String](props)

    val reporter = KafkaReporter(
      bootstrapServerUrl = "localhost:9092",
      topic = "my_topic",
      compression = None
    )

    reporter.report(report)
  }
}