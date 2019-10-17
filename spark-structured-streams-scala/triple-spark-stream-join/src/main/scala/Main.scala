import Broker.KafkaTripleJoinConsumer

object Main {


  def main(args: Array[String]) {

    println("Spark Kafka Triple Join Consumer Test program...")

    KafkaTripleJoinConsumer.runSparkConsumer
  }
}
