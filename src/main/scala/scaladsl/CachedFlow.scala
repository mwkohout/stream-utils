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
import scala.jdk.FutureConverters.*
import scala.jdk.javaapi

object CachedFlow {

  /** A cache that is shared across all materializations
    *
    * @param cache
    * @return
    */
  def apply[I, O, KEY, CACHED](
      keyExtractor: (I => KEY),
      cache: ConcurrentMap[KEY, CACHED],
      flow: Flow[I, O, NotUsed]
  )(using toCached: Conversion[Future[O], CACHED])(using toFuture: Conversion[CACHED, Future[O]]): Flow[I, O, NotUsed] = Flow
    .fromGraph(
      new CachedFlow[I, O, KEY, CACHED](
        keyExtractor,
        toCached,
        toFuture,
        () => cache,
        flow
      )
    )
    .flatMapConcat(Source.future)
}

given [T]: Conversion[Future[T], CompletionStage[T]] with
  override def apply(f: Future[T]): CompletionStage[T] =
    f.asJava

given [T]: Conversion[T, T] with
  override def apply(t: T): T = t

given [T]: Conversion[CompletionStage[T], Future[T]] with
  override def apply(cs: CompletionStage[T]): Future[T] =
    cs.asScala

given [T]: Conversion[CompletionStage[T], CompletionStage[T]] with
  override def apply(cs: CompletionStage[T]): CompletionStage[T] = cs

class CachedFlow[I, O, KEY, CACHED](
    val keyExtractor: (I => KEY),
    val toCacheType: (Future[O] => CACHED),
    val toFuture: (CACHED => Future[O]),
    val cacheBuilder: (() => ConcurrentMap[KEY, CACHED]),
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
              toFuture(
                cache.computeIfAbsent(
                  keyExtractor(value),
                  (k => {
                    val f = Source
                      .single(value)
                      .via(calculatorFlow)
                      .runWith(Sink.head[O])(materializer)
                    val handled = f.transform {
                      case f @ Failure(_) =>
                        cache.remove(k) // don't cache failures
                        f
                      case s @ Success(_) => s // nothing to do
                    }(materializer.executionContext)
                    toCacheType(handled)
                  })
                )
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
