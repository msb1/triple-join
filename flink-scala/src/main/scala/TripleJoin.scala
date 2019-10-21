
import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment


object TripleJoin {

  val threshold = 200

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "192.168.21.5:9092")
  properties.setProperty("group.id", "barnwaldo")

  // case classes for deserialization
  case class Card(cardId: Int, cardTitle: String, creationDate: Long, createdByUserId: Int, verificationInterval: Long)

  case class Verification(verificationId: Int, cardId: Int, verificationDate: Long, verifiedByUserId: Int)

  case class User(userId: Int, userName: String)

  case class UserCounts(userId: Int, userName: String, counts: Long)

  // Jackson Deserialize to type
  val objectMapper = new ObjectMapper() with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)

  def jsonToType[T](json: String)(implicit m: Manifest[T]): T = {
    objectMapper.readValue[T](json)
  }


  def runJoiner {

    // initialize stream and table environments
    val settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    // create stream of verifications
    val verificationStream = env
      .addSource(new FlinkKafkaConsumer("verifications", new SimpleStringSchema(), properties))
      .map(v => jsonToType[Verification](v))

    // create (streaming) table from verifications stream and register with "Flink Central"
    val verificationTable = tEnv.fromDataStream(verificationStream).as("verificationId, cardId, verificationDate, verifiedByUserId")
    tEnv.registerTable("Verifications", verificationTable)

    // create stream of cards
    val cardStream = env
      .addSource(new FlinkKafkaConsumer("cards", new SimpleStringSchema(), properties))
      .map(c =>  jsonToType[Card](c))

    // create (streaming) table from cards stream and register with "Flink Central"
    val cardTable = tEnv.fromDataStream(cardStream).as("id, title, creationDate, createdByUserId")
    tEnv.registerTable("Cards", cardTable)

    // create stream of users
    val userStream = env
      .addSource(new FlinkKafkaConsumer("users", new SimpleStringSchema(), properties))
      .map(u =>  jsonToType[User](u))

    // create (streaming) table from user stream and register with "Flink Central"
    val userTable = tEnv.fromDataStream(userStream).as("userId, userName")
    tEnv.registerTable("Users", userTable)

    // join verification and card tables on cardId
    val verifiedCardTable = verificationTable
      .join(cardTable).where("id = cardId")
      .select("id, verifiedByUserId, cardId, title, verificationDate")

    // join verifiedCards with users on userId; register with "Flink Central"
    val userVerifiedCardTable = verifiedCardTable
      .join(userTable).where("verifiedByUserId = userId")
      .select("userId, userName, cardId, title, verificationDate")
    tEnv.registerTable("UserVerifiedCards", userVerifiedCardTable)

    // perform table query on UserVerifiedCards for aggregation, count and filter on user (id, name)
    val results = tEnv.sqlQuery("SELECT userId, userName, COUNT(*) FROM UserVerifiedCards GROUP BY userId, userName HAVING COUNT(*) > " + threshold)

    // print results
    val stream = tEnv.toRetractStream[UserCounts](results)
      .map((v) => println(s"User ${v._2.userName} with id ${v._2.userId} has ${v._2.counts} counts"))

    // execute program
    env.execute("Flink Scala Kafka Consumer")

  }

}

