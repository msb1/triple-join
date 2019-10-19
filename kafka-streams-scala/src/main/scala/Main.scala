import java.time.Duration

import org.apache.kafka.streams.KafkaStreams
import ch.qos.logback.classic.{Level, Logger}
import org.slf4j.LoggerFactory
import streams.TripleStreamJoiner.{createTopology, props}

object Main {


  def main(args: Array[String]) {

    // set Logger levels to WARN (to avoid excess verbosity)
    LoggerFactory.getLogger("org").asInstanceOf[Logger].setLevel(Level.WARN)
    LoggerFactory.getLogger("akka").asInstanceOf[Logger].setLevel(Level.WARN)
    LoggerFactory.getLogger("kafka").asInstanceOf[Logger].setLevel(Level.WARN)

    println("Kafka Streams Triple Join Test program...")

    // initialize and start Kafka Streams
    val topology = createTopology()
    val streams: KafkaStreams = new KafkaStreams(topology, props)
    streams.start()

    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(30))
    }
  }
}