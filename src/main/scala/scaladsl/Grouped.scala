package scaladsl

import akka.stream.impl.ContextPropagation
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{
  GraphStage,
  GraphStageLogic,
  InHandler,
  OutHandler,
  TimerGraphStageLogic
}

import java.time.Instant
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.*

object Grouped {}

/**
 * WIP
 *
 * @param pushLogic
 * @param bufferConverter
 * @param interval
 * @tparam I
 * @tparam PENDINGBUFFER
 */
class Grouped[I, PENDINGBUFFER](
                                 val pushLogic: (PENDINGBUFFER) => Boolean,
                                 val bufferConverter: mutable.Buffer[I] => PENDINGBUFFER,
                                 val interval: FiniteDuration
                               ) extends GraphStage[FlowShape[I, Seq[I]]] {

  val in = Inlet[I]("Grouped.in")
  val out = Outlet[Seq[I]]("Grouped.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) {

      object periodicKey {}

      private val batch: mutable.Buffer[I] = mutable.Buffer.empty;
      private var lastPush: Option[Instant] = None

      override def preStart(): Unit = {
        scheduleOnce(periodicKey, interval)
      }

      override def onTimer(timerKey: Any): Unit = {
        val isAvail = isAvailable(out)
        val satisfiesLogic = pushLogic(bufferConverter(batch))
        val atLeastOneElement = batch.nonEmpty
        val meetsDuration = atLeastOneElement && lastPush.exists(lastPush =>
          Instant
            .now()
            .minus(interval.toJava)
            .toEpochMilli >= lastPush.toEpochMilli
        )
        if isAvail && (satisfiesLogic || meetsDuration)
        then {
          emit(out, batch.toSeq)
          lastPush = Some(Instant.now())
          batch.clear()
        }

        scheduleOnce(periodicKey, interval)

      }

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            if !hasBeenPulled(in) && !isClosed(in) then {
              pull(in)
            }
          }
        }
      )

      setHandler(
        in,
        new InHandler {
          override def onUpstreamFinish(): Unit = {

            if batch.nonEmpty then {
              emit(out, batch.toSeq)
            }
            completeStage()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            if batch.nonEmpty then {
              emit(out, batch.toSeq)
            }
            completeStage()

          }

          override def onPush(): Unit = {
            val value = grab(in)
            batch.append(value)
            if pushLogic(bufferConverter(batch)) then {
              emit(out, batch.toSeq)
              lastPush = Some(Instant.now())
              batch.clear()
              scheduleOnce(periodicKey, interval)

            }
            pull(in)

          }
        }
      )
    }

}
