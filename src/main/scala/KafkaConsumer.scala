import java.util
import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._

object KafkaConsumer extends App {

  val  props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(util.Collections.singletonList("logs"))
  val records = consumer.poll(100000).asScala.map{
    record=>{
      (record.value().split(" ")(0) ++ " " ++record.value().split(" ")(1),
      record.value().split(" ")(2).replace("[","").replace("]","")  match {
        case "info" => "info"
        case "warn" => "warn"
        case "error" => "error"
        case _ => "other"
      },
      record.value())
    }
  }.groupBy(record => (record._2, record._1)).mapValues ( _.size )
  records.foreach(record => println(record))
}