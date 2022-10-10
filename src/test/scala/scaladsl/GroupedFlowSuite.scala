package scaladsl

import akka.actor.ActorSystem
import akka.stream.DelayOverflowStrategy
import akka.stream.scaladsl.*

import java.time.Instant
import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.concurrent.duration.FiniteDuration
import scala.language.{implicitConversions, postfixOps}

class GroupedFlowSuite extends munit.FunSuite {


  test("slow trickle") {

    implicit val actorSystem: ActorSystem = ActorSystem()
    import akka.stream.Materializer.matFromSystem
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global


    val pushLogic: (Seq[Int]) => Boolean = (batch: Seq[Int]) => {
      val sizeCriteria = batch.nonEmpty
      sizeCriteria
    }
    val future = Source(List(1, 2, 3, 4, 5))
      .flatMapConcat(Source.single(_).delay(500 milli))
      .via(
        Flow.fromGraph(new Grouped[Int, Seq[Int]](pushLogic, _.toSeq, 10 milli

        ))
      ).runWith(Sink.seq)

    val result = Await.result(future, 5 minutes)

    assertEquals(result.length, 5)
    assertEquals(result, Vector(Seq(1), Seq(2), Seq(3), Seq(4), Seq(5)))
  }


  test("chunky trickle") {

    implicit val actorSystem: ActorSystem = ActorSystem()
    import akka.stream.Materializer.matFromSystem
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global


    val pushLogic: (Seq[Int]) => Boolean = (batch: Seq[Int]) => {
      val sizeCriteria = batch.length > 1
      sizeCriteria
    }
    val future = Source(List(1, 2, 3, 4, 5))
      .flatMapConcat(i => Source(List(i, i)).initialDelay(50 milli))
      .via(
        Flow.fromGraph(new Grouped[Int, Seq[Int]](pushLogic, _.toSeq, 1 milli

        ))
      ).runWith(Sink.seq)

    val result = Await.result(future, 5 minutes)

    assertEquals(result.length, 5)
    assertEquals(result, Vector(Seq(1, 1), Seq(2, 2), Seq(3, 3), Seq(4, 4), Seq(5, 5)))
  }


  test("adds up to 4") {

    implicit val actorSystem: ActorSystem = ActorSystem()
    import akka.stream.Materializer.matFromSystem
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global


    val pushLogic: (Seq[Int]) => Boolean = (batch: Seq[Int]) => {
      val sizeCriteria = batch.sum >= 4
      sizeCriteria
    }
    val future = Source(List(1, 2, 3, 4, 5))
      .flatMapConcat(i => Source(List(i, i)).initialDelay(50 milli))
      .via(
        Flow.fromGraph(new Grouped[Int, Seq[Int]](pushLogic, _.toSeq, 10 milli

        ))
      ).runWith(Sink.seq)

    val result = Await.result(future, 5 minutes)

    assertEquals(result.length, 7)
    assertEquals(result, Vector(Seq(1, 1, 2), Seq(2), Seq(3, 3), Seq(4), Seq(4), Seq(5), Seq(5)))
  }
}
