import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object Main {

  /**

    * The method creates a streaming dataframe that:
    * - connects to the specified kafka bootstrap servers
    * - subscribes from specified kafka topic
    * - reads from the specified topic offsets
    * - reads maximum number of offsets per trigger
    * @param spark the spark session
    * @param bootstrap a string with the kafka bootstrap servers
    * @param topic the kafka topic
    * @param startingOffsets the starting kafka topic offsets
    * @param maxOffsets maximum number of offsets per trigger
    * @return a streaming dataframe
    */
  def ingestKafkaTopic(spark: SparkSession, bootstrap: String, topic: String, startingOffsets: String, maxOffsets: Long): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",bootstrap)
      .option("startingOffsets",startingOffsets)
      .option("maxOffsetsPerTrigger",maxOffsets)
      .option("subscribe",topic)
      .load()
      .select(
        col("key").cast("string"),
        col("value").cast("string")
      )


  }

  /**
    * This method extracts data from the records coming from Kafka
    * @param df the input dataframe
    * @return resulting dataframe
    */
  def extractValues(df: DataFrame): DataFrame = {
    df.withColumn("tmp",split(col("value"),","))
      .select(
        col("key").as("source"),
        col("tmp").getItem(0).cast("integer").as("year"),
        col("tmp").getItem(1).cast("integer").as("month"),
        col("tmp").getItem(2).cast("integer").as("day"),
        col("tmp").getItem(3).cast("integer").as("hour"),
        col("tmp").getItem(5).cast("double").as("pm"),
        col("tmp").getItem(11).cast("double").as("temp")
      )
      .withColumn(
        "timestamp",
        unix_timestamp(
          format_string("%04d-%02d-%02d %02d:00:00",col("year"),
            col("month"),column("day"),column("hour"))).cast("timestamp")
        )
      .drop("year","month","day","hour")
      .filter(col("pm").isNotNull)
  }

  /**
    * This method calculates the top pollution event by week and source.
    * @param df the input dataframe
    * @return the resulting dataframe
    */


  def calculateTopPollutionEventsPerWeek(df: DataFrame): DataFrame = {

     df.withWatermark("timestamp", "2 weeks")
        .groupBy(col("source"),window(col("timestamp"), "1 week").alias("window"))
        .agg(max(col("pm")).alias("max_pm"),avg(col("temp")).alias("mean_temp"))
        .select(col("source"), col("window").getField("start").alias("start_timestamp"),
        col("window").getField("end").alias("end_timestamp"), col("max_pm"), round(col("mean_temp")).alias("mean_temp"))

  }

}
