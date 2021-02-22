package com.ltrojanowski.oddkeyvaluesfinder

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SaveMode, SparkSession}

import java.net.URI
import scala.io.Source

object AwsHelpers {

  def loadAmazonCredentials(): Map[String, String] = {
    val source = Source.fromFile(s"${System.getProperty("user.home")}/.aws/credentials")
    try {
      source
        .getLines()
        .filter(_.contains("="))
        .map(
          propLine =>
            propLine.split(" = ").toList match {
              case key :: value :: Nil => (key, value)
              case _                   => throw new Exception("Incorrectly formatted credentials")
            }
        )
        .toMap
    } finally {
      source.close()
    }
  }

  def awsS3SparkSession(
      awsAccessKeyId: String,
      awsSecretAccessKey: String
  ): SparkSession = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("com.ltrojanowski.oddkeyvaluesfinder")
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", awsAccessKeyId)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", awsSecretAccessKey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    spark.sparkContext.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", classOf[org.apache.hadoop.fs.s3a.S3AFileSystem].getName)
    spark
  }

  case class KeyValue(key: Int, value: Option[Int])

  def readFileIntoDataset(inputPath: String)(implicit spark: SparkSession): Dataset[KeyValue] = {
    import spark.implicits._
    val csvsRows = spark.sparkContext
      .wholeTextFiles(inputPath + "*.csv")
      .map {
        case (_, fileContent) => {
          fileContent.lines.toList.tail
        }
      }
      .fold(List.empty[String]) { case (acc, ds) => acc.union(ds) }
      .toDS
    val csvs = spark.read
      .option("header", "false")
      .option("delimiter", ";")
      .schema(Encoders.product[KeyValue].schema)
      .csv(csvsRows)
      .as[KeyValue]
    val tsvsRows = spark.sparkContext
      .wholeTextFiles(inputPath + "*.tsv")
      .map {
        case (_, fileContent) => {
          fileContent.lines.toList.tail
        }
      }
      .fold(List.empty[String]) { case (acc, ds) => acc.union(ds) }
      .toDS
    val tsvs = spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(Encoders.product[KeyValue].schema)
      .csv(tsvsRows)
      .as[KeyValue]
    csvs.union(tsvs)
  }

  def timestampAndSafeToS3(result: DataFrame, outputPath: String)(implicit spark: SparkSession) = {

    val dateTime = System.currentTimeMillis()

    result
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .mode(SaveMode.Overwrite)
      .save(outputPath + s"temp${dateTime.toString}.tsv")

    val fs    = FileSystem.get(new URI(outputPath), spark.sparkContext.hadoopConfiguration)
    val file  = fs.globStatus(new Path(s"${outputPath}temp${dateTime}.tsv/part*"))(0).getPath().getName()
    print(file)
    fs.rename(
      new Path(outputPath + s"temp${dateTime.toString}.tsv/" + file),
      new Path(outputPath + s"result${dateTime.toString}.tsv")
    )
    fs.delete(new Path(outputPath + s"temp${dateTime.toString}.tsv"), true)

  }
}
