import Broker.KafkaProducer
import Models.Generator.GenClass.{StartGenCards, StartGenUsers, StartGenVerifications}
import Models.Generator.GenData
import akka.actor.{ActorSystem, Props}
import ch.qos.logback.classic.{Level, Logger}
import org.slf4j.LoggerFactory

object Main {

  def main(args: Array[String]) {
    // set Akka Logger levels to WARN (to avoid excess verbosity)
    LoggerFactory.getLogger("org").asInstanceOf[Logger].setLevel(Level.WARN)
    LoggerFactory.getLogger("akka").asInstanceOf[Logger].setLevel(Level.WARN)
    LoggerFactory.getLogger("kafka").asInstanceOf[Logger].setLevel(Level.WARN)

    val numCards = 5000
    val numUsers = 100
    val numVerifications = 20000

    val userTopic = "users"
    val cardTopic = "cards"
    val verificationTopic = "verifications"

    println("Kafka Triple Join Demo Program...")

    //Create actors for Kafka Producer, EPD Simulator and SparkLauncher
    val system = ActorSystem("TripleJoin")
    val kafkaProducer = system.actorOf(Props[KafkaProducer], "kafkaProducer")
    val genData = system.actorOf(Props(new GenData(kafkaProducer)), "genData")

    // Generate Card, Verification and User data - send to Kafka Producer
    genData ! StartGenUsers(numUsers, userTopic) 
    genData ! StartGenCards(numCards, numUsers, cardTopic)
    genData ! StartGenVerifications(numVerifications, numUsers, verificationTopic) 
  }
}

