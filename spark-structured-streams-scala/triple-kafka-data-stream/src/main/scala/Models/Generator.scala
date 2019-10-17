package Models

import java.io.ByteArrayOutputStream

import Broker.KafkaProducerClass.{Message, Terminate}
import akka.actor.{Actor, ActorRef, DeadLetter}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import scala.util.Random

object Generator {

  case class Card(cardId:Int, cardTitle:String, creationDate:Long, createdByUserId:Int, verificationInterval:Long)

  case class Verification(verificationId:Int, cardId:Int, verificationDate:Long, verifiedByUserId:Int)

  case class User(userId:Int, userName:String)

  val objectMapper = new ObjectMapper() with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)

  def jsonToType[T](json: String)(implicit m: Manifest[T]): T = {
    objectMapper.readValue[T](json)
  }

  // random alpha string generator
  val alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
  val size = alpha.size
  val randomString: Int => String = n => (1 to n).map(x => alpha(Random.nextInt.abs % size)).mkString

  // generates list of json string users for streaming joins
  val generateUsers: Int => Array[String] = num => {
    val users = new Array[String](num)
    for(i <- 1 to num) {
      val out = new ByteArrayOutputStream()
      val user = User(i, randomString(Random.nextInt(5) + 6))
      objectMapper.writeValue(out, user)
      // println("USER string -> " + out.toString)
      users(i - 1) = out.toString
    }
    users
  }

  // generate array of json strings cards for streaming joins
  val generateCards:(Int, Int) => Array[String] = (numCards,numUsers) => {
    val cards = new Array[String](numCards)
    var timestamp: Long = (System.currentTimeMillis / 1000) - numCards * 3600
    for(i <- 1 to numCards) {
      val out = new ByteArrayOutputStream()
      timestamp += Random.nextInt(400) + 3400
      val verificationInterval = System.currentTimeMillis / 1000 - timestamp
      val card = Card(i, randomString(Random.nextInt(6) + 6), timestamp, Random.nextInt(numUsers + 1) + 1, verificationInterval)
      objectMapper.writeValue(out, card)
      // println("USER string -> " + out.toString)
      cards(i - 1) = out.toString
    }
    cards
  }

  // generate array of json string verifications for streaming joins
  val generateVerifications:(Int, Int, Array[Card]) => Array[String] = (num, numUsers, cards) => {
    val verifications = new Array[String](num)
    val numCards = cards.size
    for(i <- 1 to num) {
      val out = new ByteArrayOutputStream()
      val idx = Random.nextInt(numCards)
      val card = cards(idx)
      val timestamp = card.creationDate + 3600 + Random.nextInt(28800) + 7200
      val verifiedBy = Random.nextInt(numUsers)
      val ver =  Verification(i, card.cardId, timestamp, verifiedBy)
      objectMapper.writeValue(out, ver)
      // println("USER string -> " + out.toString)
      verifications(i - 1) = out.toString
    }
    verifications
  }

  // Actor class for data record generator
  class GenData (producer: ActorRef) extends Actor {
    import GenClass._
    var genCards = new Array[Card](10)

    def receive: Receive = {

      case StartGenUsers(numUsers, topic) => {
        val values:Array[String] = Generator.generateUsers(numUsers)
        for (value <- values) {
          val user = jsonToType[User](value)
          val key = user.userId.toString
          // val msgStr = topic + "," + key + "," + value
          producer ! Message(topic, key, value)
          println(s"USER: $topic  $key   $value")
        }
      }

      case StartGenCards(numCards, numUsers, topic) => {
        genCards = new Array[Card](numCards)
        val values:Array[String] = Generator.generateCards(numCards, numUsers)
        for (i <- 0 until numCards) {
          val value = values(i)
          genCards(i) = jsonToType[Card](values(i))
          val key = genCards(i).cardId.toString
          // val msgStr = topic + "," + key + "," + value
          producer ! Message(topic, key, value)
          println(s"CARD: $topic  $key   $value")
        }
      }

      case StartGenVerifications(numVerifications, numUsers, topic) => {
        val values:Array[String] = Generator.generateVerifications(numVerifications, numUsers, genCards)
        for (value <- values) {
          val verification = jsonToType[Verification](value)
          val key = verification.verificationId.toString
          // val msgStr = topic + "," + key + "," + value
          producer ! Message(topic, key, value)
          println(s"VERIFICATION: $topic  $key   $value")
        }
      }

      case StopGenData => {
        producer ! Terminate
        println("GenData stopped...")
      }

      case d: DeadLetter => {
        println(s"DeadLetterMonitorActor : saw dead letter $d")
      }

      case _ => println("GenData Actor received something unexpected...")
    }
  }

  object GenClass {
    case class StartGenUsers(numUsers: Int, topic: String)
    case class StartGenCards(numCards: Int, numUsers:Int, topic: String)
    case class StartGenVerifications(numVerifications: Int, numCards:Int, topic: String)
    case object StopGenData
  }

}
