import TripleJoin._
import ch.qos.logback.classic.{Level, Logger}
import org.slf4j.LoggerFactory

object Main extends App {

  // set Logger levels to WARN (to avoid excess verbosity)
  LoggerFactory.getLogger("org").asInstanceOf[Logger].setLevel(Level.WARN)
  LoggerFactory.getLogger("akka").asInstanceOf[Logger].setLevel(Level.WARN)
  LoggerFactory.getLogger("kafka").asInstanceOf[Logger].setLevel(Level.WARN)

  println("Kafka Streams Triple Join Test program...")

  // run Triple Join app
  runJoiner

}
