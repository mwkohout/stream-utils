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
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.jdk.FutureConverters.*
import scala.jdk.javaapi
import scala.concurrent.duration.FiniteDuration

object KeyedBatch{

  def apply[I,K,ICOLLECTION,OFUTURE,O, OCOLLECTION](
                                        maxElements:Int,
                                        batchWait:FiniteDuration,
                                        key:I=>K,
                                        batchEvaluator:(K,ICOLLECTION)=>OFUTURE)
                                      (using toFuture:Conversion[OFUTURE,Future[O]])
                                      (using toOCollection:Conversion[Map[K,O],OCOLLECTION])
                                      (using toICollection:Conversion[Seq[I],ICOLLECTION]):Flow[I,OCOLLECTION,NotUsed] ={

    val grouped: Flow[I, Seq[I], NotUsed] = Flow.apply[I].groupedWithin(maxElements,batchWait);

    val batch: Flow[Seq[I], Future[Map[K,O]], NotUsed] = Flow.fromGraph(
      new KeyedBatch[I, K, ICOLLECTION, OFUTURE, O](
        key, (_:ActorSystem, key:K, iCollection:ICOLLECTION)=>batchEvaluator(key,iCollection),
        toICollection,toFuture
    ));
    grouped.via(batch).flatMapConcat((o:Future[Map[K,O]])=> Source.future(o)).map(toOCollection)

  }

}
class KeyedBatch[I,K,ICOLLECTION,OFUTURE,O]
    (key: I => K,
     batchEvaluator: (ActorSystem,K, ICOLLECTION) => OFUTURE,
     toICollection:Seq[I]=>ICOLLECTION,
     toFuture:OFUTURE=>Future[O]) extends GraphStage[FlowShape[Seq[I], Future[Map[K,O]]]] {

  val in = Inlet[Seq[I]]("KeyedForkJoin.in")
  val out = Outlet[Future[Map[K,O]]]("KeyedForkJoin.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      val actorSystem = materializer.system
      implicit val ec:ExecutionContext = actorSystem.dispatcher
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val batch = grab(in)
          val futures: Map[K,Future[(K,O)]] = batch.groupBy(key)
            .map((k, seq)=>(k,toFuture(batchEvaluator(actorSystem,k,toICollection(seq))).map(o=>(k,o))(actorSystem.dispatcher)));
          val values = Future.sequence(futures.values)
          val batchMap = values.map(results=> Map.from(results))
          emit(out,batchMap)
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