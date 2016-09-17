package io.sattvik.sagas

import akka.NotUsed
import akka.event.Logging
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Source, Zip, ZipWith2}
import akka.stream._
import io.sattvik.sagas.impl.{SagaStage, SagaToFlowStage}

import scala.util.{Failure, Success, Try}

object SagaFlow {
  def fromFlows[In, Out](forward: Graph[FlowShape[In, Out], _],
                         rollback: Graph[FlowShape[Out, Any], _]): Graph[SagaShape[In, Out], NotUsed] = {

    val forwardTryFlow: Flow[Try[In],Try[Out],NotUsed] =
      Flow[Try[In]]
        .flatMapConcat {
          case Success(in) ⇒
            Source.single(in)
              .via(forward)
              .map(Success(_))
              .recover { case t ⇒ Failure[Out](t) }
          case Failure(t) ⇒
            Source.single(Failure[Out](t))
        }

    val fullRollbackFlow: Flow[(Try[Out],Option[Throwable]), Option[Throwable], NotUsed] =
      Flow[(Try[Out],Option[Throwable])]
        .flatMapConcat {
          case (Failure(_), ex) ⇒
            // we failed, so just pass the exception back
            Source.single(ex)

          case (Success(_), None) ⇒
            // we succeeded, but there was no error, so just pass none back
            Source.single(None)
          case (Success(s), ex @ Some(_)) ⇒
            // we succeeded, but upstream failed, so rollback
            Source.single(s)
                .via(rollback)
                .map(_ ⇒ ex)
        }

    GraphDSL.create(forwardTryFlow, fullRollbackFlow)(Keep.none) { implicit b ⇒ (fwd, rb) ⇒
      import GraphDSL.Implicits._

      val outBroadcast = b.add(Broadcast[Try[Out]](2, eagerCancel = false))
      val rollbackIn = b.add(Zip[Try[Out],Option[Throwable]])

      fwd.out ~> outBroadcast.in
      outBroadcast.out(0) ~> rollbackIn.in0
      rollbackIn.out ~> rb.in

      SagaShape(fwd.in, outBroadcast.out(1), rollbackIn.in1, rb.out)
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

      def tryFlow: Flow[In,Try[Out], NotUsed] = {
        val g =
          GraphDSL.create(theGraph) { implicit b ⇒ (g) ⇒
            import GraphDSL.Implicits._

            val splitter = b.add(new Broadcast[Try[Out]](2, eagerCancel = false))

            g.out ~> splitter
            val rollbackValue =
              splitter.out(0).map {
                case Success(_) ⇒ None
                case Failure(t) ⇒ Some(t)
              }
            rollbackValue ~> g.downstreamRollback

            val outputCollector = b.add(new ZipWith2[Try[Out],Option[Throwable],Try[Out]](Keep.left))

            splitter.out(1) ~> outputCollector.in0
            g.upstreamRollback ~> outputCollector.in1

            val inTransform = b.add(Flow[In].map(Success(_)))
            inTransform.out ~> g.in

            FlowShape(inTransform.in, outputCollector.out)
          }
        Flow.fromGraph(g)
      }
    }
  }
}
