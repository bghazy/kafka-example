import java.util.Properties
import java.util.regex.Pattern
import java.util.Date
import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyWindowStore}
import scala.util.{Failure, Success, Try}
import akka.http.scaladsl._
import akka.stream.scaladsl._
import scala.concurrent.Future
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._

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
      (value(0) + " " + value(1),
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

  def storeWindowed(streams: KafkaStreams, level: String, fromKey: Long, toKey: Long): String = {
    var result = ""
    var search = fromKey
    Thread.sleep(6000L)
    while (search < toKey) {
      try {
        val store: ReadOnlyWindowStore[String, Long] = streams.store(level, QueryableStoreTypes.windowStore[String, Long]())
        val timeFrom = 0L
        val timeTo = System.currentTimeMillis
        val iterator = store.fetch((search).toString, timeFrom, timeTo)
        var count = 0L
        while ( {
          iterator.hasNext
        }) {
          val next = iterator.next
          count = next.value
        }
        if (count > 0) result += new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date(search)) + ":" + count + "<br/>"
        search = search + 60000L
      } catch {
        case _: InvalidStateStoreException => {
          Thread.sleep(100)
        }
      }
    }
    result
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

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher
  val serverSource: Source[Http.IncomingConnection, Future[Http.ServerBinding]] = Http().bind(interface = "localhost", port = 8080)

  val requestHandler: HttpRequest => HttpResponse = {
    case HttpRequest(GET, Uri.Path("/"), _, _, _) =>
      HttpResponse(entity = HttpEntity(
        ContentTypes.`text/html(UTF-8)`,
        "<html><body>Hello world!</body></html>"))

    case HttpRequest(GET, Uri.Path("/info"), _, _, _) =>
      HttpResponse(entity = HttpEntity(
        ContentTypes.`text/html(UTF-8)`,
        "<html><body>" + storeWindowed(streams, "info", getTimestamp("2017/10/04 03:28:00").getTime, getTimestamp("2017/10/04 10:12:00").getTime) + "</body></html>"
      ))

    case HttpRequest(GET, Uri.Path("/warn"), _, _, _) =>
      HttpResponse(entity = HttpEntity(
        ContentTypes.`text/html(UTF-8)`,
        "<html><body>" + storeWindowed(streams, "warn", getTimestamp("2017/10/04 03:28:00").getTime, getTimestamp("2017/10/04 10:12:00").getTime) + "</body></html>"
      ))

    case HttpRequest(GET, Uri.Path("/error"), _, _, _) =>
      HttpResponse(entity = HttpEntity(
        ContentTypes.`text/html(UTF-8)`,
        "<html><body>" + storeWindowed(streams, "error", getTimestamp("2017/10/04 03:28:00").getTime, getTimestamp("2017/10/04 10:12:00").getTime) + "</body></html>"
      ))

    case r: HttpRequest =>
      r.discardEntityBytes() // important to drain incoming HTTP Entity stream
      HttpResponse(404, entity = "Unknown resource!")
  }

  val bindingFuture: Future[Http.ServerBinding] =
    serverSource.to(Sink.foreach { connection =>
      println("Accepted new connection from " + connection.remoteAddress)

      connection handleWithSyncHandler requestHandler
      // this is equivalent to
      // connection handleWith { Flow[HttpRequest] map requestHandler }
    }).run()
}