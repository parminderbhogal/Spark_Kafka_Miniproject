import java.io.File

import kafka.admin.AdminUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.scalatest.FlatSpec
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._

class MainTest extends FlatSpec with Eventually with TestEnvironment {

  // Kafka Topic Name -> PM
  val topic: String = "pm"

  // Kafka bootstrap server
  var bootstrap: String = _

  // Loading data into Kafka topic <pm>
  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create topic
    AdminUtils.createTopic(server.zkUtils, topic, 1, 1)

    // Get bootstrap address of server
    bootstrap = s"${server.config.hostName}:${server.config.advertisedPort}"

    // Push input data through Kafka
    // Data is sorted by time
    val schema = StructType(Array(
      StructField("source", StringType),
      StructField("value", StringType)
    ))
    val rdd = new File(this.getClass.getResource("/").getFile)
      .listFiles()
      .flatMap { file =>
        if (file.getName.endsWith(".csv")) {
          val df = spark.read
            .option("header", true)
            .option("inferSchema", true)
            .option("nullValue", "NA")
            .csv(file.getPath)
            .withColumn("source", lit(file.getName))
          Some(df.select("source", "year", "month", "day", "hour", "season", "PM_US Post", "DEWP", "HUMI", "PRES", "TEMP", "cbwd", "Iws", "precipitation", "Iprec"))
        } else {
          None
        }
      }
      .reduce(_ union _)
      .coalesce(1)
      .sort("year", "month", "day", "hour")
      .rdd
      .map { row =>
        val rowSeq = row.toSeq
        Row.fromSeq(Seq(rowSeq.head, rowSeq.tail.mkString(",")))
      }
    spark.createDataFrame(rdd, schema)
      .select(lit(topic).as("topic"), col("source").as("key"), col("value"))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .save()
  }

  behavior of "Main"

  "ingestKafkaTopic" must "generate the correct schema" in {
    val schema = StructType(
      Array(
        StructField("key", StringType),
        StructField("value", StringType)
      )
    )
    val df = Main.ingestKafkaTopic(spark, bootstrap, topic, "latest", 1l)

    assert(df.schema == schema)
  }

  "ingestKafkaTopic" must "ingest values from kafka" in {
    Main.ingestKafkaTopic(spark, bootstrap, topic, s"""{"$topic":{"0":-2}}""", 1l)
      .writeStream
      .outputMode("append")
      .format("memory")
      .queryName("kafka_output")
      .trigger(Trigger.Once())
      .start()

    // Wait for stream to process records and verify content of in-memory table
    eventually(timeout(30 seconds), interval(10 seconds)) {



      val rows = spark.table("kafka_output").collect()
      rows.foreach(println)
      assert(rows.length == 1)
      assert(rows(0).getAs[String]("key") == "BeijingPM20100101_20151231.csv")
      assert(rows(0).getAs[String]("value") == "2010,1,1,0,4,null,-21.0,43.0,1021.0,-11.0,NW,1.79,0.0,0.0")
    }
  }

  "extractValues" must "generate the correct schema" in {
    val schema = StructType(
      Array(
        StructField("source", StringType),
        StructField("pm", DoubleType),
        StructField("temp", DoubleType),
        StructField("timestamp", TimestampType)
      )
    )
    val df = Main.ingestKafkaTopic(spark, bootstrap, topic, "latest", 1l)
    val values = Main.extractValues(df)

    assert(values.schema == schema)
  }

  "extractValues" must "extract values from dataframe" in {
    val rdd = spark.sparkContext.parallelize(
      Seq(
        Row.fromSeq(Seq("BeijingPM20100101_20151231.csv", "2010,1,1,23,4,129,-17,41,1020,-5,cv,0.89,0,0"))
      )
    )
    val schema = StructType(
      Array(
        StructField("key", StringType),
        StructField("value", StringType)
      )
    )
    val df = spark.createDataFrame(rdd, schema)
    val row = Main.extractValues(df).head()
    println(row)
    assert(row.getAs[String]("source") == "BeijingPM20100101_20151231.csv")
    assert(row.getAs[Double]("pm") == 129d)
    assert(row.getAs[Double]("temp") == 0.89d)
  }


  "calculateTopPollutionEventsPerWeek" must "generate the correct schema" in {
    val schema = StructType(
      Array(
        StructField("source", StringType),
        StructField("start_timestamp", TimestampType),
        StructField("end_timestamp", TimestampType),
        StructField("max_pm", DoubleType),
        StructField("mean_temp", DoubleType)
      )
    )
    val df = Main.ingestKafkaTopic(spark, bootstrap, topic, "latest", 1)
    val values = Main.extractValues(df)
    val output = Main.calculateTopPollutionEventsPerWeek(values)

    assert(output.schema == schema)
  }


  "Main" must "calculate correct aggregations on all data when processing end to end" in {

    // Setup application and read from earliest offset
    // Forward output to an in-memory table
    val df = Main.ingestKafkaTopic(spark, bootstrap, topic, "earliest", Long.MaxValue)
    val values = Main.extractValues(df)
    Main.calculateTopPollutionEventsPerWeek(values)
      .writeStream
      .outputMode("update")
      .format("memory")
      .queryName("pm_aggs")
      .start()
      .processAllAvailable()

    // Wait for stream to process records and verify content of in-memory table
    eventually(timeout(60 seconds), interval(10 seconds)) {


      val rows = spark.table("pm_aggs").sort(col("max_pm").desc).limit(5).collect()
      rows.foreach(println)
      assert(rows.length == 5)
      assert(rows(0).getAs[String]("source") == "BeijingPM20100101_20151231.csv")
      assert(rows(0).getAs[Double]("max_pm") == 994d)
      assert(rows(0).getAs[Double]("mean_temp") == 28d)
      assert(rows(1).getAs[String]("source") == "BeijingPM20100101_20151231.csv")
      assert(rows(1).getAs[Double]("max_pm") == 980d)
      assert(rows(1).getAs[Double]("mean_temp") == 17)
      assert(rows(2).getAs[String]("source") == "ShenyangPM20100101_20151231.csv")
      assert(rows(2).getAs[Double]("max_pm") == 932d)
      assert(rows(2).getAs[Double]("mean_temp") == 23d)
      assert(rows(3).getAs[String]("source") == "BeijingPM20100101_20151231.csv")
      assert(rows(3).getAs[Double]("max_pm") == 886d)
      assert(rows(3).getAs[Double]("mean_temp") == 10d)
      assert(rows(4).getAs[String]("source") == "ShenyangPM20100101_20151231.csv")
      assert(rows(4).getAs[Double]("max_pm") == 864d)
      assert(rows(4).getAs[Double]("mean_temp") == 29d)
    }


  }


}
