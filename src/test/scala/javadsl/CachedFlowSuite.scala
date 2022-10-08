package javadsl

import akka.stream.javadsl.*
import akka.actor.ActorSystem

import java.util.concurrent.{CompletionStage, ConcurrentHashMap}

class CachedFlowSuite extends munit.FunSuite {

  test("when the java dsl is used it works"){

    val cache = new ConcurrentHashMap[Integer,CompletionStage[String]]();

    val cachedFlow = Flow.fromFunction((i:Integer)=>i.toString)

    val result: CompletionStage[String] = Source.single[Integer](1)
      .via(CachedFlow.apply((i: Integer) => i, cache = ()=>cache, calculator = cachedFlow, config = CachedFlow.Config(false)))
      .runWith(Sink.head(), ActorSystem.apply());


    assertEquals(result.toCompletableFuture.get(), "1")




  }

}
