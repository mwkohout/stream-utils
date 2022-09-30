package scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.util.{Failure, Success}
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.CompletionStage
import scala.concurrent.{ExecutionContextExecutor, Future}

object CachedFlow {

  /** A cache that is shared across all materializations
    *
    * @param cache
    * @return
    */
  def apply[I, O, KEY](
      keyExtractor: (I => KEY),
      cache: ConcurrentMap[KEY, Future[O]],
      flow: Flow[I, O, NotUsed]
  ): Flow[I, O, NotUsed] = Flow
    .fromGraph(new CachedFlow[I, O, KEY](keyExtractor, () => cache, flow))
    .flatMapConcat(Source.future(_))
}

class CachedFlow[I, O, KEY](
    val keyExtractor: (I => KEY),
    val cacheBuilder: (() => ConcurrentMap[KEY, Future[O]]),
    val calculatorFlow: Flow[I, O, NotUsed]
) extends GraphStage[FlowShape[I, Future[O]]] {

  val in             = Inlet[I]("CachedFlow.in")
  val out            = Outlet[Future[O]]("CachedFlow.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      val cache = cacheBuilder()
      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {

            val value = grab(in)
            push(
              out,
              cache.computeIfAbsent(
                keyExtractor(value),
                (k => {
                  val f = Source
                    .single(value)
                    .via(calculatorFlow)
                    .runWith(Sink.head[O])(materializer)
                  f.onComplete { case Failure(ex) =>
                    cache.remove(k) // don't cache failures
                  }(materializer.executionContext)
                  f
                })
              )
            )
          }
        }
      )
      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            pull(in)
          }
        }
      )

    }
}
