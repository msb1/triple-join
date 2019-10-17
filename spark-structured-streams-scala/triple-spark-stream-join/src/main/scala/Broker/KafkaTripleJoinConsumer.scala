package Broker

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}


object KafkaTripleJoinConsumer {

  val userTopic = "users"
  val cardTopic = "cards"
  val verificationTopic = "verifications"
  val threshold = 200

  // case classes for triple join
  case class Card(cardId: Int, cardTitle: String, creationDate: Long, createdByUserId: Int, verificationInterval: Long)

  case class Verification(verificationId: Int, cardId: Int, verificationDate: Long, verifiedByUserId: Int)

  case class User(userId: Int, userName: String)

  case class VerifiedCard(cardId: Int, cardTitle: String, userId: Int, userName: String)

  // json deserialize function for scala
  val objectMapper = new ObjectMapper() with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)

  def jsonToType[T](json: String)(implicit m: Manifest[T]): T = {
    objectMapper.readValue[T](json)
  }

  // Spark SQL struct schema for simulated data
  val cardSchema = new StructType()
    .add("cardId", IntegerType)
    .add("cardTitle", StringType)
    .add("creationDate", LongType)
    .add("createdByUserId", IntegerType)
    .add("verificationInterval", LongType)

  val verificationSchema = new StructType()
    .add("verificationId", IntegerType)
    .add("cardId", IntegerType)
    .add("verificationDate", LongType)
    .add("verifiedByUserId", IntegerType)

  val userSchema = new StructType()
    .add("userId", IntegerType)
    .add("userName", StringType)


  def runSparkConsumer: Unit = {
    // create or get SparkSession
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("KafkaStructuredStreamTester")
      .getOrCreate()
    import spark.implicits._

    // Subscribe to topics with spark kafka connector
    // Streaming card data
    val cards = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.21.5:9092")
      .option("subscribe", cardTopic)
      .option("startingOffsets", "earliest")
      .option("includeTimestamp", value = true)
      .load()

    //Streaming verification data
    val verifications = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.21.5:9092")
      .option("subscribe", verificationTopic)
      .option("startingOffsets", "earliest")
      .option("includeTimestamp", value = true)
      .load()

    // streaming user data
    val users = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.21.5:9092")
      .option("subscribe", userTopic)
      .option("startingOffsets", "earliest")
      .option("includeTimestamp", value = true)
      .load()

    // streaming verified cards (includes key=cardId and value=userName)
    val verifiedCards = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.21.5:9092")
      .option("subscribe", "verifiedCards")
      .option("startingOffsets", "earliest")
      .load()

    // Transform card stream data into dataframe
    val cardQuery: DataFrame = cards.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
      .as[(String, String, Timestamp)]
      .select($"key", $"timestamp".as("cardTimestamp"), from_json($"value", cardSchema).as("card"))
      .selectExpr("card.cardId", "card.cardTitle", "cardTimestamp", "card.creationDate", "card.createdByUserId", "card.verificationInterval")

    // Transform verifications stream into dataframe
    val verificationQuery: DataFrame = verifications.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
      .as[(String, String, Timestamp)]
      .select($"key", $"timestamp".as("veriTimestamp"), from_json($"value", verificationSchema).as("veri"))
      .selectExpr("veri.verificationId", "veri.cardId", "veriTimestamp", "veri.verificationDate", "veri.verifiedByUserId")

    // Transform users stream into datafra,e
    val userQuery: DataFrame = users.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
      .as[(String, String, Timestamp)]
      .select($"key", $"timestamp".as("userTimestamp"), from_json($"value", userSchema).as("user"))
      .selectExpr("user.userId", "user.userName", "userTimestamp")

    // inner join card dataframe with verification dataframe on cardId
    val join1: DataFrame = cardQuery.join(verificationQuery, "cardId")
    // inner join previous join to user with userId
    val join2: DataFrame = join1.join(userQuery, $"verifiedByUserId" === $"userId")

    // send joined to data to kafka producer since stream-stream joins can only be aggregated on watermarked timestamp column
    // in append mode; a clean stream form a kafka consumer can be directly aggregated in complete mode
    val kafkaProducer = join2.select($"userId".cast(DataTypes.StringType).as("key"), $"userName".cast(DataTypes.StringType).as("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.21.5:9092")
      .option("topic", "verifiedCards")
      .option("checkpointLocation", "/home/bw/scala/Spark/KafkaSparkTripleJoin/checkpoints")
      .start()

    // with verifiedCard data received from Kafka Consumer, perform aggregation - groupby, count and filter
    // then output dataframe to console in batches
    val verifiedCardQuery = verifiedCards.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .select($"key".as("userId"), $"value".as("userName"))
      .groupBy($"userName")
      .count()
      .filter($"count" > threshold)
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    kafkaProducer.awaitTermination()
    verifiedCardQuery.awaitTermination()
    kafkaProducer.stop()
    verifiedCardQuery.stop()

  }
}

