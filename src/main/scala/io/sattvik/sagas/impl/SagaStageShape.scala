package io.sattvik.sagas.impl

import akka.stream.{Inlet, Outlet, Shape}

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.immutable.Seq
import scala.util.Try

private[sagas] final case class SagaStageShape[-In,+Out] (in: Inlet[In @uncheckedVariance],
                                                          forwardIn: Outlet[In @uncheckedVariance],
                                                          forwardOut: Inlet[Out @uncheckedVariance],
                                                          out: Outlet[Out @uncheckedVariance],
                                                          downstreamRollback: Inlet[Option[Throwable]],
                                                          rollbackIn: Outlet[Out @uncheckedVariance],
                                                          rollbackOut: Inlet[Any],
                                                          upstreamRollback: Outlet[Option[Throwable]]) extends Shape {
  override val inlets: Seq[Inlet[_]] = List(in, downstreamRollback, forwardOut, rollbackOut)
  override val outlets: Seq[Outlet[_]] = List(out, upstreamRollback, forwardIn, rollbackIn)

  override def deepCopy(): SagaStageShape[In,Out] =
    SagaStageShape(
      in.carbonCopy(),
      forwardIn.carbonCopy(),
      forwardOut.carbonCopy(),
      out.carbonCopy(),
      downstreamRollback.carbonCopy(),
      rollbackIn.carbonCopy(),
      rollbackOut.carbonCopy(),
      upstreamRollback.carbonCopy())

  override def copyFromPorts(inlets: Seq[Inlet[_]], outlets: Seq[Outlet[_]]): Shape = {
    require(inlets.size == 4, s"proposed inlets [${inlets.mkString(", ")}] do not fit SagaStageShape")
    require(outlets.size == 4, s"proposed outlets [${outlets.mkString(", ")}] do not fit SagaStageShape")

    SagaStageShape(
      inlets(0).asInstanceOf[Inlet[In]],
      outlets(2).asInstanceOf[Outlet[In]],
      inlets(2).asInstanceOf[Inlet[Out]],
      outlets(0).asInstanceOf[Outlet[Out]],
      inlets(1).asInstanceOf[Inlet[Option[Throwable]]],
      outlets(3).asInstanceOf[Outlet[Out]],
      inlets(3).asInstanceOf[Inlet[Any]],
      outlets(1).asInstanceOf[Outlet[Option[Throwable]]])
  }
}
