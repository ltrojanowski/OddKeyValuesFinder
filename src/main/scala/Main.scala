package com.ltrojanowski.oddkeyvaluesfinder

import org.apache.spark.sql.SparkSession

object Main {

  import AwsHelpers._
  import Logic._

  case class Parameters(inputPath: String, outputPath: String)

  def extractInputAndOutputPath(args: Array[String]): Parameters = {
    args match {
      case Array(input, output) => Parameters(input, output)
      case _                    => throw new Exception("Wrong number of inputs")
    }
  }

  def main(args: Array[String]): Unit = {
    val parameters        = extractInputAndOutputPath(args)
    val amazonCredentials = loadAmazonCredentials()
    val awsAccessKeyId = amazonCredentials.getOrElse(
      "aws_access_key_id",
      throw new Exception("Missing aws_access_key_id property in ~/.aws/properties")
    )
    val awsSecretAccessKey = amazonCredentials.getOrElse(
      "aws_secret_access_key",
      throw new Exception("Missing aws_secret_access_key property in ~/.aws/properties")
    )

    implicit val spark: SparkSession = awsS3SparkSession(awsAccessKeyId, awsSecretAccessKey)
    import spark.implicits._
    val keyValues = readFileIntoDataset(parameters.inputPath)

    //This is how I would actually implemented this problem. I think it reads well
    val result = keyValues
      .groupByKey(_.key)
      .mapGroups {
        case (key, values) => (key, findNumberOccurringOddNumberOfTimes(values.map(_.value.getOrElse(0)).toIterable))
      }
      .toDF("key", "number_occurring_odd_amount_of_times")

    // this is to show that I know that groupBy could potentially cause shuffling related problems
    // so combineByKey is a good alternative
    /*
    val result = keyValues
      .map(kv => KeyValue.unapply(kv).get)
      .rdd.combineByKey(
      value => IntMap[Int](value.getOrElse(0) -> 1),
      (values: IntMap[Int], value: Option[Int]) => {
        val v = value.getOrElse(0)
        val currentCount = values.getOrElse(v, 0)
        values.+((v, currentCount + 1))
      },
      (m1: IntMap[Int], m2: IntMap[Int]) => m1.unionWith(m2, (_, c1, c2) => c1 + c2))
      .mapValues(valuesCount => keyOfOddValue(valuesCount))
      .toDF("key", "number_occurring_odd_amount_of_times")
     */

    timestampAndSafeToS3(result, parameters.outputPath)
  }

}
