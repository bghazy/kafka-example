/*
package com.tenzki.akkahttpkafkastream

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import scala.concurrent.ExecutionContext
import scala.io.StdIn
import scala.util.{Failure, Success}

object Main extends App {

  val config: Config = ConfigFactory.load()

  implicit val actorSystem = ActorSystem()
  implicit val executor: ExecutionContext = actorSystem.dispatcher

  implicit val log: LoggingAdapter = Logging(actorSystem, getClass)
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val producerSettings = ProducerSettings(actorSystem, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")

  val overflowStrategy = akka.stream.OverflowStrategy.backpressure

  val source = Source.queue[(String, String)](1000, akka.stream.OverflowStrategy.backpressure)
    .map(elem => new ProducerRecord[Array[Byte], String](elem._1, elem._2))
    .to(Producer.plainSink(producerSettings))
    .run()

  val route = pathSingleSlash {
    get {
      complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Akka HTTP stream Kafka example</h1>"))
    }
  } ~
    (post & path(Remaining) & entity(as[String])) { (path, entity) =>
      onComplete(source.offer((path, entity))) {
        case Success(value) => complete(s"$value")
        case Failure(ex)    => complete((StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}"))
      }
    }

  val host = config.getString("http.host")
  val port = config.getInt("http.port")
  val bindingFuture = Http().bindAndHandle(route, host, port)
  println(s"Server online at http://$host:$port/\nPress RETURN to stop...")
  StdIn.readLine()
  bindingFuture.flatMap(_.unbind()).onComplete(_ => actorSystem.terminate())

}*/
