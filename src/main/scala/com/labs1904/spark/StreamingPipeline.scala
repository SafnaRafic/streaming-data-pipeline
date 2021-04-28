package com.labs1904.spark

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Spark Structured Streaming app
 *
 */
object StreamingPipeline {

  case class Review(marketplace: String, customer_id: String, review_id: String, product_id: String, product_parent: String, product_title: String, product_category: String, star_rating: String, helpful_votes: String, total_votes: String, vine: String, verified_purchase: String, review_headline: String, review_body: String, review_date: String) //#2
  case class User(name: String, username: String, sex: String, birthdate: String) //#3
  case class Both(user: User, review: Review)

  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "StreamingPipeline"

  implicit def stringToBytes(s: String): Array[Byte] = Bytes.toBytes(s)

  implicit def bytesToString(bytes: Array[Byte]): String = Bytes.toString(bytes)

  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder().appName(jobName).master("local[*]").getOrCreate()

      // TODO: change bootstrap servers to your kafka brokers
      // Question 1- Ingest data from a "reviews" Kafka topic.
      val bootstrapServers = "35.239.241.212:9092,35.239.230.132:9092,34.69.66.216:9092"

      // To convert a dataframe to a dataset- import spark.implicits._
      import spark.implicits._ //#2
      val df = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", "reviews")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "20")
        .load()
        .selectExpr("CAST(value AS STRING)").as[String]
      //      df.printSchema()

      // Question 2 - Parse the comma separated values into a Review scala case class
      val reviews = df.map(r => {
        val reviewsAsList = r.split(",")
        Review(reviewsAsList(0),
          reviewsAsList(1),
          reviewsAsList(2),
          reviewsAsList(3),
          reviewsAsList(4),
          reviewsAsList(5),
          reviewsAsList(6),
          reviewsAsList(7),
          reviewsAsList(8),
          reviewsAsList(9),
          reviewsAsList(10),
          reviewsAsList(11),
          reviewsAsList(12),
          reviewsAsList(13),
          reviewsAsList(14)
        )
      })
      reviews.printSchema()

      //Question 3 - Extract the userId from the Review object. - customer_Id
      val userData = reviews.mapPartitions(partition => {

        // Connecting to Hbase (cloud)
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "cdh01.hourswith.expert:2181,cdh02.hourswith.expert:2181,cdh03.hourswith.expert:2181")
        val connection = ConnectionFactory.createConnection(conf)
        val table = connection.getTable(TableName.valueOf("srafic:users"))

        val iteration = partition.map(row => {

          // Question 4 - Use the userId to lookup the corresponding user data in HBase.
          val get = new Get(row.customer_id).addFamily("f1")
          val result = table.get(get)
          // println(result) - For checking the fields in the table

          // Getting user data from Hbase
          val name = result.getValue("f1", "name")
          val username = result.getValue("f1", "username")
          val sex = result.getValue("f1", "sex")
          val birthdate = result.getValue("f1", "birthdate")
          val user = User(name, username, sex, birthdate)

          //Question 5 - Join the review data with the user data.
          Both(user, row)

        }).toList.iterator

        connection.close()
        iteration

      })

      val query = userData.writeStream
        .outputMode(OutputMode.Append())
        .format("console")
        .option("truncate", false)
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start()

      query.awaitTermination()

    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }
}


