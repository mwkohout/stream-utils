package scaladsl

import akka.stream.scaladsl.*

import scala.language.implicitConversions
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}

import java.util.Timer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.*

class KeyedBatchSuite extends munit.FunSuite {


  test("when I have a stream of letters, I batch up and count how many there are") {

    val a2z: List[String] = (('a' to 'z').toList ++: ('a' to 'z' by 2).toList).map(_.toString())
    implicit val actorSystem = ActorSystem.apply()
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

    val letterCountF = Source(a2z).via(
      KeyedBatch(a2z.length, 2.seconds, (i: String) => i,
        (key: String, collection: Seq[String]) => Future {
          collection.length
        },

      )).runWith(Sink.seq[Map[String, Int]])

    val letterCountResults = Await.result(letterCountF, 2.seconds)

    assertEquals(letterCountResults.length, 1)

    assertEquals(letterCountResults.toList(0), Map("e" -> 2,
      "n" -> 1,
      "t" -> 1,
      "a" -> 2,
      "m" -> 2,
      "i" -> 2,
      "v" -> 1,
      "p" -> 1,
      "r" -> 1,
      "w" -> 2,
      "k" -> 2,
      "s" -> 2,
      "x" -> 1,
      "j" -> 1,
      "y" -> 2,
      "u" -> 2,
      "f" -> 1,
      "q" -> 2,
      "b" -> 1,
      "g" -> 2,
      "l" -> 1,
      "c" -> 2,
      "h" -> 1,
      "o" -> 2,
      "z" -> 1,
      "d" -> 1))

  }


}
