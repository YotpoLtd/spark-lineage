package org.chojin.spark.lineage.reporter

import org.chojin.spark.lineage.inputs.{Field, HiveInput, How}
import org.chojin.spark.lineage.outputs.FsOutput
import org.chojin.spark.lineage.report.{Metadata, Report}

object MockReport {
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
}
