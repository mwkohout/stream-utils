package scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.DelayOverflowStrategy
import akka.stream.StreamRefMessages.ActorRef
import com.google.common.cache.CacheBuilder
import akka.stream.scaladsl.*
import akka.stream.javadsl.Sink as JSink
import akka.testkit.TestProbe
import scaladsl.*
import scaladsl.CachedFlow.Config

import java.util.concurrent.{CompletionStage, ConcurrentHashMap, CountDownLatch}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.FutureConverters.*
import scala.language.implicitConversions

// For more information on writing tests, see
// https://scalameta.org/munit/docs/getting-started.html
class CachedFlowSuite extends munit.FunSuite {

  test("when the user provides a future generating function, avoiding the actorsystem's executors"){
    implicit val actorSystem: ActorSystem = ActorSystem()
    import akka.stream.Materializer.matFromSystem
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

    val cache = CacheBuilder.newBuilder().maximumSize(100).build[Int,Future[String]]().asMap()

    val cacheFunction:(Int=>Future[String])= i=>Future{i.toString}

    val results: Seq[String] = Source(List(1, 1))
      .via(CachedFlow( (i: Int) => i, cache = () => cache, valueExtractor = cacheFunction))
      .runWith(Sink.seq[String])(matFromSystem(ActorSystem())).asJava.toCompletableFuture.get();
  }
  test("when a entry goes through a flow then it's cached as a Future") {

    implicit val actorSystem: ActorSystem = ActorSystem()
    import akka.stream.Materializer.matFromSystem
    val cache = CacheBuilder.newBuilder().maximumSize(100).build[Int,Future[String]]().asMap()
    val testKit:TestProbe = TestProbe()
    val cachedFlow: Sink[Int, Future[String]] = Flow.fromFunction[Int, String](i => i.toString).map(s=>{
      testKit.ref.tell(s,akka.actor.Actor.noSender)
      s
    })
      .toMat(Sink.head)(Keep.right)
    val results:Seq[String] = Source(List(1,1))
      .via(CachedFlow({(i:Int)=>i},cache = ()=> cache, valueExtractor = cachedFlow))
      .runWith(Sink.seq[String])(matFromSystem(ActorSystem())).asJava.toCompletableFuture.get();

    //we get the correct result
    assertEquals(Seq("1", "1"),results)
    assertEquals(1, cache.keySet().size())

    //it only goes through the cache calculator once for this key
    testKit.expectMsg("1")
    testKit.expectNoMessage()

    //the cache contains what we'd expect
    assert( cache.keySet()contains(1))
    assert(cache.get(1).isInstanceOf[Future[String]])

  }

  /**
   * this seems sort of ecoteric--that someone would push a scala source into a java sink, then convert it back but it's a legit case.
   */
  test("when a entry goes through a flow then it's cached as a CompletionStage") {

    import akka.stream.Materializer.matFromSystem
    val cache = CacheBuilder.newBuilder().maximumSize(100).build[Int, CompletionStage[String]]().asMap()
    val cachedFlow: Sink[Int, CompletionStage[String]] = Flow.fromFunction[Int, String](i => i.toString).toMat(JSink.head().asScala)(Keep.right)
    val result: String = Source.single(1)
      .via(CachedFlow( (i: Int) => i, cache = ()=>cache, valueExtractor = cachedFlow))
      .runWith(Sink.head[String])(matFromSystem(ActorSystem())).asJava.toCompletableFuture.get();
    assertEquals("1", result)
    assertEquals(1, cache.keySet().size())
    assert(cache.keySet() contains (1))
    assert(cache.get(1).isInstanceOf[CompletionStage[String]])
  }


  test("when the flow is configured to discard errors") {

    import akka.stream.Materializer.matFromSystem
    import concurrent.ExecutionContext.Implicits.global
    import concurrent.duration.DurationInt

    val cache = new ConcurrentHashMap[Int,Future[String]]()
    val cachedFlow: Sink[Int, Future[String]] = Flow.fromFunction[Int, String](i => i.toString)
      .flatMapConcat(s=>Source.failed(new Exception())).toMat(Sink.head)(Keep.right)
    intercept[Exception] {
      val f = Source.single(1)
        .via(CachedFlow({ (i: Int) => i }, cache = ()=>cache, valueExtractor = cachedFlow))
        .delay(1.second, DelayOverflowStrategy.backpressure)
        .runWith(Sink.seq[String])(matFromSystem(ActorSystem()))
      Await.result(f,1.second)
    }

    assertEquals( cache.keySet().size(),0)

  }
  test("when the flow is configured to keep errors in cache") {

    import akka.stream.Materializer.matFromSystem
    import concurrent.ExecutionContext.Implicits.global
    import concurrent.duration.DurationInt

    val cache = new ConcurrentHashMap[Int, Future[String]]()
    val cachedFlow: Sink[Int, Future[String]] =
      Flow.fromFunction[Int, String](i => i.toString)
        .flatMapConcat(s => Source.failed(new Exception()))
        .toMat(Sink.head)(Keep.right)
    intercept[Exception] {
      val f = Source.single(1)
        .via(CachedFlow( (i: Int) => i , cache = ()=> cache, valueExtractor = cachedFlow)(using Config(cacheFailure = true)))
        .delay(1.second, DelayOverflowStrategy.backpressure)
        .runWith(Sink.seq[String])(matFromSystem(ActorSystem()))
      Await.result(f, 1.second)
    }

    assertEquals(cache.keySet().size(), 1)
    assert(cache.keySet() contains (1))

    intercept[Exception]{
      Await.result(cache.get(1), 1.second)
    }

  }

}
