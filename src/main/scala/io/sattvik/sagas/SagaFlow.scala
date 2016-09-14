package io.sattvik.sagas

import akka.NotUsed
import akka.event.Logging
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Source}
import akka.stream._
import io.sattvik.sagas.impl.{SagaStage, SagaToFlowStage}

import scala.util.{Failure, Success, Try}

object SagaFlow {
  def fromFlows[In, Out](forward: Graph[FlowShape[In, Out], _],
                         rollback: Graph[FlowShape[Out, _], _]): Graph[SagaShape[In, Out], NotUsed] = {
    val forwardWithRecovery =
      Flow.fromGraph(forward)
          .map[Try[Out]](Success(_))
          .recoverWithRetries(5, {
            case t ⇒ Source.single(Failure(t))
          })
          .withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))


    GraphDSL.create(forward, rollback)(Keep.none) { implicit b ⇒ (fwd, rb) ⇒
      import GraphDSL.Implicits._

      val stage = b.add(new SagaStage[In, Out])

      stage.forwardIn ~> fwd ~> stage.forwardOut
      stage.rollbackIn ~> rb ~> stage.rollbackOut

      SagaShape(stage.in, stage.out, stage.downstreamRollback, stage.upstreamRollback)
    }
  }

  object Implicits {
    implicit class SagaFlowOps[In,Out](val theGraph: Graph[SagaShape[In, Out], NotUsed]) extends AnyVal {
      def atop[Out2](next: Graph[SagaShape[Out,Out2], NotUsed]): Graph[SagaShape[In,Out2],NotUsed] = {
          GraphDSL.create(theGraph, next)(Keep.none) { implicit b ⇒ (g1, g2) ⇒
            import GraphDSL.Implicits._

            g1.out ~> g2.in
            g2.upstreamRollback ~> g1.downstreamRollback

            SagaShape(g1.in, g2.out, g2.downstreamRollback, g1.upstreamRollback)
          }
      }

      def toFlow: Flow[In,Out, NotUsed] = {
        val g =
          GraphDSL.create(theGraph) { implicit b ⇒ (g) ⇒
            import GraphDSL.Implicits._

            val sagas = b.add(new SagaToFlowStage[In, Out])
            sagas.intoFlow ~> g.in
            g.out ~> sagas.fromFlow
            sagas.intoRollback ~> g.downstreamRollback
            g.upstreamRollback ~> sagas.fromRollback

            FlowShape(sagas.in, sagas.out)
          }
        Flow.fromGraph(g)
      }
    }
  }
}
