package javadsl

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.javadsl.{Flow as JFlow, Sink as JSink, Source as JSource}

import scala.concurrent.Future
import scaladsl.CachedFlow

import java.util.concurrent.{CompletionStage, ConcurrentMap}
import java.util.function.Supplier
import scala.annotation.static
object CachedFlow {

  @static case class Config(cacheFailure: java.lang.Boolean)

  /**
   *
   * @param keyExtractor
   * @param cache
   * @param calculator Takes the first result from the flow to cache.  To cache multiple results in the flow, apply Flow.collect
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
    import scaladsl.given_Conversion_Future_CompletionStage
    import scaladsl.given_Conversion_CompletionStage_Future

    val calc: Sink[I,Future[O]] = calculator.asScala.toMat(Sink.head)(Keep.right)

    scaladsl.CachedFlow
      .apply(
        keyExtractor = keyExtractor.apply,
        cache =cache.get,
        calculator = calc,
        config = scaladsl.CachedFlow.Config(cacheFailure = config.cacheFailure)
      )
      .asJava
  }
}
