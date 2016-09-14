package io.sattvik.sagas.impl

import akka.stream.{Inlet, Outlet, Shape}

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.immutable.Seq

private[sagas] final case class SagaToFlowShape[-In,+Out](in: Inlet[In @uncheckedVariance],
                                                          out: Outlet[Out @uncheckedVariance],
                                                          intoFlow: Outlet[In @uncheckedVariance],
                                                          fromFlow: Inlet[Out @uncheckedVariance],
                                                          intoRollback: Outlet[Option[Throwable]],
                                                          fromRollback: Inlet[Option[Throwable]]) extends Shape {

  override val inlets: Seq[Inlet[_]] = List(in, fromFlow, fromRollback)

  override val outlets: Seq[Outlet[_]] = List(out, intoFlow, intoRollback)

  override def deepCopy(): SagaToFlowShape[In,Out] =
    SagaToFlowShape(
      in.carbonCopy(),
      out.carbonCopy(),
      intoFlow.carbonCopy(),
      fromFlow.carbonCopy(),
      intoRollback.carbonCopy(),
      fromRollback.carbonCopy())

  override def copyFromPorts(inlets: Seq[Inlet[_]], outlets: Seq[Outlet[_]]): SagaToFlowShape[In,Out] = {
    require(inlets.size == 3, s"proposed inlets [${inlets.mkString(", ")}] do not fit SagaToFlowShape")
    require(outlets.size == 3, s"proposed outlets [${outlets.mkString(", ")}] do not fit SagaToFlowShape")

    SagaToFlowShape(
      inlets(0).asInstanceOf[Inlet[In]],
      outlets(0).asInstanceOf[Outlet[Out]],
      outlets(1).asInstanceOf[Outlet[In]],
      inlets(1).asInstanceOf[Inlet[Out]],
      outlets(2).asInstanceOf[Outlet[Option[Throwable]]],
      inlets(2).asInstanceOf[Inlet[Option[Throwable]]])
  }
}
