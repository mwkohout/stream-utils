package scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import scaladsl.CachedFlow.Config

import scala.util.{Failure, Success}
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.CompletionStage
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.FutureConverters.*
import scala.jdk.javaapi

object CachedFlow {

  case class Config(cacheFailure:Boolean)
  /**
    *
    * @param cache
    * @return
    */
  def apply[I, O, KEY, CACHED](
                                keyExtractor: I => KEY,
                                cache: ()=>ConcurrentMap[KEY, CACHED],
                                valueExtractor: Sink[I, CACHED])
                              (using config: Config)
                              (using toCached: Conversion[Future[O], CACHED])
                              (using toFuture: Conversion[CACHED, Future[O]]): Flow[I, O, NotUsed] = {
    Flow.fromGraph(
      new CachedFlow[I, O, KEY, CACHED](
        keyExtractor,
        toCached,
        toFuture,
        cache,
        config,
        (system:ActorSystem,input:I)=> Source.single(input).runWith[CACHED](valueExtractor)(Materializer(system))
      )
    )
    .flatMapConcat(Source.future)
  }

  def apply[I, O, KEY, CACHED](
                              keyExtractor: I => KEY,
                              cache: () => ConcurrentMap[KEY, CACHED],
                              valueExtractor: I=>CACHED)
                              (using config: Config)
                              (using toCached: Conversion[Future[O], CACHED])
                              (using toFuture: Conversion[CACHED, Future[O]]): Flow[I, O, NotUsed] = {
  Flow.fromGraph(
    new CachedFlow[I, O, KEY, CACHED](
      keyExtractor,
      toCached,
      toFuture,
      cache,
      config,
      (_: ActorSystem, input: I) => valueExtractor(input)
    )
  )
  .flatMapConcat(Source.future)
}
}
given DefaultCachedConfig:Config = Config(cacheFailure = false);
given FutureToCompletionStage[T]: Conversion[Future[T], CompletionStage[T]] with
  override def apply(f: Future[T]): CompletionStage[T] = f.asJava

given SelfToSelf[T]: Conversion[T, T] with
  override def apply(t: T): T = t

given CompletionStageToFuture[T]: Conversion[CompletionStage[T], Future[T]] with
  override def apply(cs: CompletionStage[T]): Future[T] =
    cs.asScala

given [T]: Conversion[CompletionStage[T], CompletionStage[T]] with
  override def apply(cs: CompletionStage[T]): CompletionStage[T] = cs

class CachedFlow[I, O, KEY, CACHED](
    val keyExtractor: (I => KEY),
    val toCacheType: (Future[O] => CACHED),
    val toFuture: (CACHED => Future[O]),
    val cacheBuilder: (() => ConcurrentMap[KEY, CACHED]),
    val config:scaladsl.CachedFlow.Config,
    val valueCalculator: (ActorSystem,I)=>CACHED
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
                    val f = toFuture(valueCalculator(materializer.system,value))
                    val handled = f.transform {
                      case f @ Failure(_) =>
                        // don't cache failures
                        if !config.cacheFailure then {cache.remove(k)}
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
