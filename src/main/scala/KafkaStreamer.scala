import java.util.Properties
import java.util.regex.Pattern
import org.apache.kafka.streams.kstream._
import org.apache.kafka.common.serialization.{Serdes, StringDeserializer, StringSerializer, _}
import org.apache.kafka.streams.KafkaStreams

object KafkaStreamer extends App {

  val  props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("zookeeper.connect", "localhost:2181")
   props.put("key.deserializer", new StringDeserializer)
  props.put("value.deserializer", new StringDeserializer)
  props.put("group.id", "something")
  props.put("application.id", "logs")

  val builder: KStreamBuilder = new KStreamBuilder
  val stringSerde: Serde[String] = Serdes.serdeFrom(new StringSerializer, new StringDeserializer)
  val longSerde: Serde[java.lang.Long] = Serdes.serdeFrom(new LongSerializer,new LongDeserializer)
  val logs: KStream[String, String] = builder.stream(stringSerde, stringSerde, "logs")
  val records: KStream[String, (String,String,String)] = logs.mapValues{
    record : String=>{
      val value = Pattern.compile(" ").split(record.toString)
      (value(0) ++ " " ++ value(1),
        value(2) match {
          case "[info]" => "info"
          case "[warn]" => "warn"
          case "[error]" => "error"
          case _ => "other"
        },
        record.toString)
    }
  }

  val levels = Array("info", "warn", "error", "other")
  levels.foreach{
    level => {
      records.filter(new Predicate[String, (String,String,String)] {
        override def test(key: String, value: (String,String,String)): Boolean = value._2 == level
      }).groupBy[String](new KeyValueMapper[String,(String,String,String),String]() {
        override def apply(key: String, value: (String,String,String)): (String) = value._1
      }).count(level).toStream.to(stringSerde, longSerde, level)
    }
  }

  val streams = new KafkaStreams(builder, props)
  streams.start()


}