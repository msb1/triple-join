package streams

import java.util.Properties

import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{StreamsConfig, Topology}
import utils.JSONSerde


object TripleStreamJoiner {

  val threshold = 200
  val cardTopic = "cards"
  val verificationTopic = "verifications"
  val userTopic = "users"

  val CARD_STORE = "card-store"
  val USER_STORE = "user-store"

  // case classes for triple join
  case class Card(cardId: Int, cardTitle: String, creationDate: Long, createdByUserId: Int, verificationInterval: Long)

  case class Verification(verificationId: Int, cardId: Int, verificationDate: Long, verifiedByUserId: Int)

  case class User(userId: Int, userName: String)

  case class VerifiedCard(verification: Verification, card: Card)

  case class UserVerifiedCard(verification: Verification, card: Card, user: User)

  // Load properties
  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "triple-join") // group.Id for Kafka Streams
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.21.5:9092")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
  props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000.asInstanceOf[Object])

  val verificationSerde = new JSONSerde[Verification]
  val cardSerde = new JSONSerde[Card]
  val userSerde = new JSONSerde[User]
  val userVerifiedCardSerde = new JSONSerde[UserVerifiedCard]

  def createTopology(): Topology = {

    val builder: StreamsBuilder = new StreamsBuilder

    // KStream with verifications - map CardId to record key
    val verificationStream: KStream[String, Verification] = builder
      .stream[String, Verification](verificationTopic)(Consumed.`with`(Serdes.String, verificationSerde))
      .map[String, Verification]((_, value) => (value.cardId.toString, value))
//      .peek((key, value) => println(s"Verification: $key  ${value.verificationId}  ${value.cardId}   ${value.verificationDate}"))


    // GlobalKTable for cards
    val cardTable: GlobalKTable[String, Card] = builder
      .globalTable[String, Card](cardTopic)(Consumed.`with`(Serdes.String, cardSerde))

    // GlobalKTable for users
    val userTable: GlobalKTable[String, User] = builder
      .globalTable[String, User](userTopic)(Consumed.`with`(Serdes.String, userSerde))

    // Inner Join verifications stream to cards table to create stream of Verified Cards - join on cardId - map userId to record key
    val verifiedCardStream: KStream[String, VerifiedCard] = verificationStream
      .join[String, Card, VerifiedCard](cardTable)((key, _) => key, (verification, card) => VerifiedCard(verification, card))
      .map[String, VerifiedCard]((_, value) => (value.verification.verifiedByUserId.toString, value))
//      .peek((key, value) => println(s"VerifiedCard: $key  ${value.verification.verificationId}  ${value.card.cardId}   ${value.card.creationDate}"))

    // Inner Join Verified Cards stream to users table to create stream of User Verified Cards - join on userId
    val userVerifiedCardStream: KStream[String, UserVerifiedCard] = verifiedCardStream
      .join[String, User, UserVerifiedCard](userTable)((key, _) => key, (verifiedCard, user) =>
        UserVerifiedCard(verifiedCard.verification, verifiedCard.card, user))
//      .peek((key, value) => println(s"USERVerifiedCard: $key  ${value.verification.verificationId}  ${value.card.cardId}   ${value.card.creationDate} ${value.user.userName}"))

    // Perform aggregate to get verifications by user (no time windowing applied for now)
    val userCounts: KTable[String, Long] = userVerifiedCardStream
      .groupBy((_, value) => value.user.userName)(Grouped.`with`(Serdes.String, userVerifiedCardSerde))
      .count()(Materialized.`with`(Serdes.String, Serdes.Long))

    // Output userCounts table to stream (when updates occur); filter out items below counts threshold, print to console
    userCounts
      .toStream
      .filter((_, value) => value > threshold)
      .foreach((userName, counts) => println(s"User: $userName -- counts: $counts"))

    builder.build()
  }

}
