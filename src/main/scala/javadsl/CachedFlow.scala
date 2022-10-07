package javadsl

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.javadsl.{Flow as JFlow, Sink as JSink, Source as JSource}
import scaladsl.CachedFlow as ScalaDSLCachedFlow

import java.util.concurrent.{CompletionStage, ConcurrentMap}
import scala.annotation.static
object CachedFlow {

  @static case class Config(cacheFailure: java.lang.Boolean)

  def apply[I, O, KEY](
      keyExtractor: java.util.function.Function[I, KEY],
      cache: ConcurrentMap[KEY, CompletionStage[O]],
      flow: JFlow[I, O, NotUsed],
      config: Config = Config(cacheFailure = false)
  ): JFlow[I, O, NotUsed] = {
    import scaladsl.given_Conversion_Future_CompletionStage
    import scaladsl.given_Conversion_CompletionStage_Future

    ScalaDSLCachedFlow
      .apply(
        keyExtractor.apply,
        cache = cache,
        flow = flow.asScala,
        config = ScalaDSLCachedFlow.Config(cacheFailure = config.cacheFailure)
      )
      .asJava
  }
}
