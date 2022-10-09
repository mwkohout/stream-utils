package scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import scaladsl.CachedFlow.Config

import java.time.Duration
import scala.util.{Failure, Success}
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.CompletionStage
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.FutureConverters.*
import scala.jdk.javaapi
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object KeyedForkJoin{

  def apply[I,K,ICOLLECTION,OFUTURE,O](
                                        maxElements:Int,
                                        batchWait:FiniteDuration,
                                        key:I=>K,
                                        batchEvaluator:(K,ICOLLECTION)=>OFUTURE)
                                      (using toFuture:Conversion[OFUTURE,Future[O]])
                                      (using toICollection:Conversion[Seq[I],ICOLLECTION]):Flow[I,O,NotUsed] ={

    val grouped: Flow[I, Seq[I], NotUsed] = Flow.apply[I].groupedWithin(maxElements,batchWait);

    val keyedForkJoin: Flow[Seq[I], Future[O], NotUsed] = Flow.fromGraph(
      new KeyedForkJoin[I, K, ICOLLECTION, OFUTURE, O](
        key, (_:ActorSystem, key:K, iCollection:ICOLLECTION)=>batchEvaluator(key,iCollection),
        toICollection,toFuture
    ));
    grouped.via(keyedForkJoin).flatMapConcat((o:Future[O])=> Source.future(o))

  }

}
class KeyedForkJoin[I,K,ICOLLECTION,OFUTURE,O]
    (key: I => K,
     batchEvaluator: (ActorSystem,K, ICOLLECTION) => OFUTURE,
     toICollection:Seq[I]=>ICOLLECTION,
     toFuture:OFUTURE=>Future[O]) extends GraphStage[FlowShape[Seq[I], Future[O]]] {

  val in = Inlet[Seq[I]]("KeyedForkJoin.in")
  val out = Outlet[Future[O]]("KeyedForkJoin.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      val actorSystem = materializer.system

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val batch = grab(in)
          val futures: Map[K,Future[O]] = batch.groupBy(key)
            .map((k, seq)=>(k,toFuture(batchEvaluator(actorSystem,k,toICollection(seq)))));
          val values = futures.values.toList
          emitMultiple(out,values)
        }
      })

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