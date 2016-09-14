package io.sattvik.sagas

import akka.stream.{Inlet, Outlet, Shape}

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.immutable.Seq
import scala.util.Try

final case class SagaShape[-In,+Out](in: Inlet[Try[In @uncheckedVariance]],
                                     out: Outlet[Try[Out @uncheckedVariance]],
                                     downstreamRollback: Inlet[Option[Throwable]],
                                     upstreamRollback: Outlet[Option[Throwable]]) extends Shape {
  override val inlets: Seq[Inlet[_]] = List(in, downstreamRollback)
  override val outlets: Seq[Outlet[_]] = List(out, upstreamRollback)

  override def deepCopy(): SagaShape[In,Out] =
    SagaShape(in.carbonCopy(), out.carbonCopy(), downstreamRollback.carbonCopy(), upstreamRollback.carbonCopy())

  override def copyFromPorts(inlets: Seq[Inlet[_]], outlets: Seq[Outlet[_]]): Shape = {
    require(inlets.size == 2, s"proposed inlets [${inlets.mkString(", ")}] do not fit SagaShape")
    require(outlets.size == 2, s"proposed outlets [${outlets.mkString(", ")}] do not fit SagaShape")

    SagaShape(
      inlets(0).asInstanceOf[Inlet[Try[In]]],
      outlets(0).asInstanceOf[Outlet[Try[Out]]],
      inlets(1).asInstanceOf[Inlet[Option[Throwable]]],
      outlets(1).asInstanceOf[Outlet[Option[Throwable]]])
  }
}
