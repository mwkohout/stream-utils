package javadsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.javadsl.{Flow as JFlow, Sink as JSink, Source as JSource}

import scala.concurrent.Future
import scaladsl.CachedFlow

import java.util.concurrent.{CompletionStage, ConcurrentMap}
import java.util.function.Supplier
import scala.annotation.static
object CachedFlow {

  @static case class Config(cacheFailure: java.lang.Boolean)

  /** @param keyExtractor
   * @param cache
   * @param calculator
   * Takes the first result from the flow to cache. To cache multiple results
   * in the flow, apply Flow.collect
   * @param config
   * @tparam I
   * @tparam O
   * @tparam KEY
   * @return
   */
  def apply[I, O, KEY](
                        keyExtractor: java.util.function.Function[I, KEY],
                        cache: Supplier[ConcurrentMap[KEY, CompletionStage[O]]],
                        calculator: JFlow[I, O, NotUsed],
                        config: Config = Config(cacheFailure = false)
                      ): JFlow[I, O, NotUsed] = {

    val calc: JSink[I, CompletionStage[O]] = calculator.toMat(
      JSink.head[O](),
      akka.stream.javadsl.Keep.right[akka.NotUsed, CompletionStage[O]]
    )

    val f: (ActorSystem, I) => CompletionStage[O] =
      (system: ActorSystem, input: I) =>
        Source.single(input).runWith(calc)(Materializer(system))
    Flow
      .fromGraph(
        new CachedFlow[I, O, KEY, CompletionStage[O]](
          keyExtractor.apply,
          scaladsl.FutureToCompletionStage,
          scaladsl.CompletionStageToFuture,
          cache.get,
          scaladsl.CachedFlow.Config(cacheFailure = config.cacheFailure),
          f
        )
      )
      .flatMapConcat(Source.future)
      .asJava
  }
}
