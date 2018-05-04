import java.net.InetSocketAddress
import java.util.Properties

import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.spark.sql.SparkSession
import org.apache.zookeeper.server.{ServerCnxnFactory, ZooKeeperServer}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Suite}

import scala.reflect.io.Directory

trait TestEnvironment extends BeforeAndAfterAll with BeforeAndAfter {
  this: Suite =>

  var spark: SparkSession = _
  var server: KafkaServer = _

  private var zkFactory: ServerCnxnFactory = _
  private val zkLogsDir = Directory.makeTemp("zookeeper-logs")
  private val kafkaLogsDir = Directory.makeTemp("kafka-logs")

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession.builder
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()

    spark.sql("set spark.sql.shuffle.partitions=10") //setting shuffle partitions to 10 for fast testing
    // Parameters
    val zkPort = 2181
    val brokerPort = 9092

    // Start ZooKeeper
    val zkServer = new ZooKeeperServer(zkLogsDir.toFile.jfile, zkLogsDir.toFile.jfile, 500)
    zkFactory = ServerCnxnFactory.createFactory
    zkFactory.configure(new InetSocketAddress("localhost", zkPort), 16)
    zkFactory.startup(zkServer)

    // Kafka Broker properties
    val properties: Properties = new Properties
    properties.setProperty("zookeeper.connect", s"localhost:$zkPort")
    properties.setProperty("broker.id", "0")
    properties.setProperty("host.name", "localhost")
    properties.setProperty("port", brokerPort.toString)
    properties.setProperty("auto.create.topics.enable", "true")
    properties.setProperty("log.dir", kafkaLogsDir.toAbsolute.path)
    properties.setProperty("log.flush.interval.messages", "1")
    properties.setProperty("replica.socket.timeout.ms", "1500")

    // Create broker and start it
    server = new KafkaServer(new KafkaConfig(properties))
    server.startup()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.stop()
    server.shutdown()
    server.awaitShutdown()
    zkFactory.shutdown()
    zkLogsDir.deleteRecursively()
    kafkaLogsDir.deleteRecursively()
  }
}
