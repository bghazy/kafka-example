import java.util.Properties
import java.util.regex.Pattern
import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyWindowStore}
import scala.util.{Failure, Success, Try}

object KafkaStreamer extends App {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", Serdes.String().getClass)
  props.put("value.deserializer", Serdes.String().getClass)
  props.put("serializer.class", "kafka.serializer.DefaultEncoder")
  props.put("application.id", "logs")

  val builder: KStreamBuilder = new KStreamBuilder
  val logs: KStream[String, String] = builder.stream(Serdes.String, Serdes.String, "logs")


  val records: KStream[String, (String, String, String)] = logs.mapValues {
    record: String => {
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

  def createFilteredKStream(records: KStream[String, (String, String, String)], level: String): KStream[String, (String, String, String)] = {
    records.filter(new Predicate[String, (String, String, String)] {
      override def test(key: String, value: (String, String, String)): Boolean = value._2 == level
    })
  }

  def createMappedKStream(records: KStream[String, (String, String, String)]): KStream[String, String] = {
    records.map[String, String] {
      new KeyValueMapper[String, (String, String, String), KeyValue[String, String]] {
        override def apply(key: String, value: (String, String, String)): KeyValue[String, String] = {
          val v = Pattern.compile(":").split(value._1)
          new KeyValue(getTimestamp(v(0) + ":" + v(1) + ":00").getTime.toString, value._3)
        }
      }
    }
  }

  def createGroupedKStream(records: KStream[String, String]): KGroupedStream[String, String] = {
    records.groupByKey(Serdes.String, Serdes.String())
  }

  def storeWindowed(streams: KafkaStreams, level: String): Unit = {
    println(level)
    var timeFrom = 0L
    var search=getTimestamp("2017/10/04 03:29:00").getTime

    while(search<getTimestamp("2017/10/04 10:12:00").getTime){
      Thread.sleep(600L)
      try {
        val store: ReadOnlyWindowStore[String, Long] = streams.store(level, QueryableStoreTypes.windowStore[String, Long]())
        val timeTo = System.currentTimeMillis


        val iterator = store.fetch((search).toString, timeFrom, timeTo)
        var count=0L
        while ( {iterator.hasNext}) {
          val next = iterator.next
          count=next.value

        }
        println(search+":"+count)
        search=search+60000L

      } catch {
        case ioe: InvalidStateStoreException => {
          //println(ioe)
          Thread.sleep(100)
        }
      }
    }
  }

  def getTimestamp(s: String): Timestamp = s match {
    case "" => new Timestamp(0)
    case _ => {
      val format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
      Try(new Timestamp(format.parse(s).getTime)) match {
        case Success(t) => t
        case Failure(_) => new Timestamp(0)
      }
    }
  }

  createGroupedKStream(createMappedKStream(createFilteredKStream(records, "error"))).count(TimeWindows.of(60 * 1000), "error")
  createGroupedKStream(createMappedKStream(createFilteredKStream(records, "info"))).count(TimeWindows.of(60 * 1000), "info")
  createGroupedKStream(createMappedKStream(createFilteredKStream(records, "warn"))).count(TimeWindows.of(60 * 1000), "warn")



  val streams: KafkaStreams = new KafkaStreams(builder, props)
  streams.start()

  storeWindowed(streams, "error")
  storeWindowed(streams, "info")
  storeWindowed(streams, "warn")

}