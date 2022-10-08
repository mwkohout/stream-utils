package scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.DelayOverflowStrategy
import com.google.common.cache.CacheBuilder
import akka.stream.scaladsl.*
import scaladsl.*

import java.util.concurrent.{CompletionStage, ConcurrentHashMap, CountDownLatch}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.FutureConverters.*
import scala.language.implicitConversions

// For more information on writing tests, see
// https://scalameta.org/munit/docs/getting-started.html
class CachedFlowSuite extends munit.FunSuite {
  test("when a entry goes through a flow then it's cached as a Future") {

    import akka.stream.Materializer.matFromSystem
    val cache = CacheBuilder.newBuilder().maximumSize(100).build[Int,Future[String]]().asMap()
    val cachedFlow: Sink[Int, Future[String]] = Flow.fromFunction[Int, String](i => i.toString).toMat(Sink.head)(Keep.right)
    val result:String = Source.single(1)
      .via(CachedFlow({(i:Int)=>i},cache = ()=> cache, calculator = cachedFlow))
      .runWith(Sink.head[String])(matFromSystem(ActorSystem())).asJava.toCompletableFuture.get();
    assertEquals("1",result)
    assertEquals(1, cache.keySet().size())
    assert( cache.keySet()contains(1))
    assert(cache.get(1).isInstanceOf[Future[String]])

  }

  test("when a entry goes through a flow then it's cached as a CompletionStage") {

    import akka.stream.Materializer.matFromSystem
    val cache = CacheBuilder.newBuilder().maximumSize(100).build[Int, CompletionStage[String]]().asMap()
    val cachedFlow: Sink[Int, Future[String]] = Flow.fromFunction[Int, String](i => i.toString).toMat(Sink.head)(Keep.right)
    val result: String = Source.single(1)
      .via(CachedFlow({ (i: Int) => i }, cache = ()=>cache, calculator = cachedFlow))
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
        .via(CachedFlow({ (i: Int) => i }, cache = ()=>cache, calculator = cachedFlow))
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
        .via(CachedFlow({ (i: Int) => i }, cache = ()=> cache, calculator = cachedFlow,CachedFlow.Config(cacheFailure = true)))
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
