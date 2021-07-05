package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
object Main {

  def main(args: Array[String]): Unit = {
    implicit val parameters: ParamsLoad = new ParamsLoad(args)
    val appName: String = "Kafka-to-Parquet"

    val spark: SparkSession = SparkSession.builder()
      .appName(appName)
      .master("local[2]")
      .config("spark.driver.memory", "5g")
      .getOrCreate()

    import spark.implicits._

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", parameters.KAFKA_SERVERS) // localhost:9092
      .option("subscribe", parameters.KAFKA_TOPIC) // smart_topic
      .option("startingOffsets", parameters.KAFKA_OFFSET) // earliest
      .load()

    val ds = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    val rawData = ds.filter($"value".isNotNull)

    val smartSchema = new StructType()
      .add(StructField("EVNUM", StringType))
      .add(StructField("EVTIME", StringType))
      .add(StructField("XML_DATA", StringType))
      .add(StructField("DATE_PART", LongType))

    val parsedData = rawData.select(from_json($"value", smartSchema).as("data")).select("data.*")

    parsedData.printSchema()

    parsedData
      .repartition(1)
      .writeStream
      .trigger(Trigger.Once())
      .outputMode("APPEND")
      .partitionBy("DATE_PART")
      .format("parquet")
      .option("path", parameters.PATH_LOCAL_SAVE)  // data/output
      .option("checkpointLocation", parameters.CHECKPOINT_LOCATION) // data/checkpoint
      .start

    spark.streams.awaitAnyTermination()
  }
}

