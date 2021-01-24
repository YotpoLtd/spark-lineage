package org.chojin.spark.lineage.reporter

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.chojin.spark.lineage.inputs.{Field, HiveInput, How}
import org.chojin.spark.lineage.outputs.FsOutput
import org.chojin.spark.lineage.report.{Metadata, Report}
import org.json4s.jackson.JsonMethods._
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.{WordSpec, Matchers => ScalaTestMatchers}

class KafkaReporterSpec extends WordSpec with MockitoSugar with ScalaTestMatchers with ArgumentMatchersSugar with EmbeddedKafka {
  val report = Report(
    Metadata("my-app"),
    FsOutput(
      path = "s3:///bucket/path/to/data",
      format = "parquet"),
    Map(
      "one" -> List(
        HiveInput(
          name = "db.table1",
          fields = Set(
            Field(name = "pk", how = How.JOIN),
            Field(name = "one", how = How.PROJECTION))),
        HiveInput(
          name = "db.table2",
          fields = Set(
            Field(name = "pk", how = How.JOIN)))),
      "two" -> List(
        HiveInput(
          name = "db.table1",
          fields = Set(
            Field(name = "pk", how = How.JOIN))),
        HiveInput(
          name = "db.table2",
          fields = Set(
            Field(name = "pk", how = How.JOIN),
            Field(name = "two", how = How.PROJECTION))))
    )
  )

  "report" should {
    "put a kafka record" in {
      implicit val config = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 12345)

      EmbeddedKafka.start()

      val reporter = KafkaReporter(
        bootstrapServerUrl = "localhost:9092",
        topic = "my_topic",
        compression = None
      )

      reporter.report(report)

      val result = consumeFirstStringMessageFrom("my_topic")
      val resultMap = parse(result).values.asInstanceOf[Map[String, Any]]

      val output = resultMap("output").asInstanceOf[Map[String, String]]
      val metadata = resultMap("metadata").asInstanceOf[Map[String, String]]
      val fields = resultMap("fields").asInstanceOf[Map[String, String]]

      metadata.get("appName").asInstanceOf[Some[String]].get shouldEqual "my-app"

      resultMap("metadata") shouldEqual Map("appName" -> "my-app")

      resultMap("output") shouldEqual
        Map(
          "typeName" -> "fs",
          "format" -> "parquet",
          "path" -> "s3:///bucket/path/to/data"
        )

      resultMap("fields") shouldEqual
        Map(
          "one" -> List(
            Map(
              "name" -> "db.table1",
              "fields" -> List(
                Map(
                  "name" -> "pk",
                  "how" -> "JOIN"
                ),
                Map(
                  "name" -> "one",
                  "how" -> "PROJECTION"
                )
              ),
              "typeName" -> "hive"
            ),
            Map(
              "typeName" -> "hive",
              "name" -> "db.table2",
              "fields" -> List(
                Map(
                  "name" -> "pk",
                  "how" -> "JOIN"
                )
              )
            )
          ),
          "two" -> List(
            Map(
              "typeName" -> "hive",
              "name" -> "db.table1",
              "fields" -> List(
                Map(
                  "name" -> "pk",
                  "how" -> "JOIN"
                )
              )
            ),
            Map(
              "typeName" -> "hive",
              "name" -> "db.table2",
              "fields" -> List(
                Map(
                  "name" -> "pk",
                  "how" -> "JOIN"
                ),
                Map(
                  "name" -> "two",
                  "how" -> "PROJECTION"
                )
              )
            )
          )
        )

      EmbeddedKafka.stop()
    }
  }
}
