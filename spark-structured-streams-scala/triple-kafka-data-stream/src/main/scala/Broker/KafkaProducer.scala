package Broker

import akka.actor.{Actor, ActorSystem}
import akka.kafka.ProducerMessage.MultiResultPart
import akka.kafka.scaladsl.Producer
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

class KafkaProducer extends Actor {

  import KafkaProducerClass._

  // define ActorSystem and Materializer for akka streams
  implicit val system = ActorSystem("TripleJoin")
  implicit val materializer = ActorMaterializer()

  // kafka producer config/settings
  val proConfig = system.settings.config.getConfig("akka.kafka.producer")
  val producerSettings: ProducerSettings[String, String] = ProducerSettings(proConfig, new StringSerializer, new StringSerializer)

  def receive: Receive = {
    case Message(topic, key, value) => {
      val singleSource = Source.single(Message(topic, key, value))
        .map { msg =>
          ProducerMessage.single(
            new ProducerRecord(msg.topic, msg.key, msg.value),
            msg.key
          )
        }
        .via(Producer.flexiFlow(producerSettings))
        .map {
          case ProducerMessage.Result(metadata, ProducerMessage.Message(record, passThrough)) =>
            s"topic=${metadata.topic}  partition=${metadata.partition}  offset=${metadata.offset}: \n${record.value}"

          case ProducerMessage.MultiResult(parts, passThrough) =>
            parts
              .map {
                case MultiResultPart(metadata, record) =>
                  s"topic=${metadata.topic}  partition=${metadata.partition}  offset=${metadata.offset}: \n${record.value}"
              }
              .mkString(", ")

          case ProducerMessage.PassThroughResult(passThrough) =>
            s"Producer message sent with key $passThrough"
        }
        .runWith(Sink.foreach(println(_)))
    }
    case Terminate => {
      system.terminate()
      println("System terminate for Kafka Producer...")
    }
    case _ => println("KafkaProducer received something unexpected... No action taken...")
  }
}

object KafkaProducerClass {

  case class Message(topic: String, key: String, value: String)

  case object Terminate

}