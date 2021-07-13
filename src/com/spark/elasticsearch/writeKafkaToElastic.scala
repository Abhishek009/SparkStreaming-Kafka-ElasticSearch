package com.spark.elasticsearch

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.SparkSession
import java.util.Date
import org.apache.spark.sql.functions._
import org.apache.spark.sql
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.kafka.common.serialization.StringDeserializer

object writeKafkaToElastic {

  def main(args: Array[String]) {

    // Setting the log level as error only.
    Logger.getLogger("org").setLevel(Level.ERROR);
     
    val brokers = "localhost:9092"
    val topics = "movie"

    val spark = SparkSession.builder().appName("writeKafkaToElastic")
      .master("local[1]")
      .getOrCreate();

    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer].toString(),
      "value.deserializer" -> classOf[StringDeserializer].toString(),
      "auto.offset.reset" -> "largest")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(topics))

    import spark.implicits._

    val lines = messages.map(_._2)

    lines.foreachRDD(
      rdd => {

        val line = rdd.toDS()

        println("Old Schema");
        line.show() /*
+------------------+
|             value|
+------------------+
| 1,1,4.0,964982703|
| 1,3,4.0,964981247|
+------------------+
      */

        // Date format
        val dateformat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        val currentdate = dateformat.format(new Date)

        /*
         1. Break the comma seperated values into columns and name them.
      	 2. Add a new column with Date
        */
        val modifiedDF = line.withColumn("_tmp", split(col("value"), ",")).select(
          $"_tmp".getItem(0).as("userId").cast(sql.types.IntegerType),
          $"_tmp".getItem(1).as("movieId").cast(sql.types.IntegerType),
          $"_tmp".getItem(2).as("rating").cast(sql.types.DoubleType),
          $"_tmp".getItem(3).as("timestamp")).withColumn("Date", lit(currentdate))

        println("With new fields")
        modifiedDF.show()
        /*
+------+-------+------+---------+-------------------+
|userId|movieId|rating|timestamp|               Date|
+------+-------+------+---------+-------------------+
|     1|      1|     4|964982703|2019-11-24T03:43:43|
|     1|      3|     4|964981247|2019-11-24T03:43:43|
+------+-------+------+---------+-------------------+*/

        modifiedDF.write
          .format("org.elasticsearch.spark.sql")
          .option("es.port", "9200") // ElasticSearch port
          .option("es.nodes", "localhost") // ElasticSearch host
          .mode("append")
          .save("movieratingspark/doc") // indexname/document type

      })

    ssc.start()
    ssc.awaitTermination()
  }
}