import java.util.concurrent.Future
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Callback
import scala.io.Source

object KafkaProducer extends App {

  val props = new java.util.Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("client.id", "KafkaProducer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)

  val filename = "host.log"
  var i = 0
  for (line <- Source.fromResource(filename).getLines) {
    i += 1
    val record = new ProducerRecord[String, String]("logs","line-"+i , line)
    val metaF: Future[RecordMetadata] = producer.send(record, new Callback{
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) {
          println(exception.toString)
        }
      }
    })
  }
  producer.close()
}