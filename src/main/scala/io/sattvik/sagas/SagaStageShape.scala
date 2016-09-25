package io.sattvik.sagas

import akka.stream.{Inlet, Outlet, Shape}

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.immutable.Seq
import scala.util.Try

/** A shape that encapsulates both a forward flow and a rollback (or
  * compensating) flow.  The forward flow takes an `In` and produces an `Out`.
  * The rollback flow takes the `Out` and performs the rollback operation,
  * returning `Done`.
  *
  * Both of these flows are packaged into a saga stage, which will manage
  * whether or not each of the component flows wil run.  In particular:
  *
  *  - If the input to the stage is a `Failure`, the forward flow will not run
  *    and the rollback will not run.
  *  - If the input to the stage is a `Success`, the forward flow will be
  *    executed.  If the forward flow succeeded, the result will be forwarded
  *    in a `Success`; otherwise, the failure will be forwarded as a `Failure`.
  *  - If the result of the saga is a [[SagaFailed]] and the forward flow
  *    succeeded, the rollback flow will be executed.  In all other cases, the
  *    rollback flow will not be executed.
  *
  * {{{
  *              +---------------------------+
  *              |        Saga Stage         |
  *              |                           |
  *              |        +----------+       |
  *    Try[In] ~>|   In ~>| Forward  |~> Out |~> Try[Out]
  *              |        +----------+       |
  *              |                           |
  *              |        +----------+       |
  * SagaResult <~| Done <~| Rollback |<~ Out |<~ SagaResult
  *              |        +----------+       |
  *              |                           |
  *              +---------------------------+
  * }}}
  *
  * @param in the inlet for the input from upstream
  * @param out the outlet for the result to be forwarded downstream
  * @param downstreamRollback the input from downstream to signal whether or not
  *                           the rollback should be run
  * @param upstreamRollback the output to which the result should be forwarded
  *                         upstream
  * @tparam In the type of the input for the forward flow
  * @tparam Out the type of the output for the forward flow
  *
  * @author Daniel Solano Gómez
  */
final case class SagaStageShape[-In,+Out](in: Inlet[Try[In @uncheckedVariance]],
                                          out: Outlet[Try[Out @uncheckedVariance]],
                                          downstreamRollback: Inlet[Option[Throwable]],
                                          upstreamRollback: Outlet[Option[Throwable]]) extends Shape {
  override val inlets: Seq[Inlet[_]] = List(in, downstreamRollback)
  override val outlets: Seq[Outlet[_]] = List(out, upstreamRollback)

  override def deepCopy(): SagaStageShape[In,Out] =
    SagaStageShape(in.carbonCopy(), out.carbonCopy(), downstreamRollback.carbonCopy(), upstreamRollback.carbonCopy())

  override def copyFromPorts(inlets: Seq[Inlet[_]], outlets: Seq[Outlet[_]]): Shape = {
    require(inlets.size == 2, s"proposed inlets [${inlets.mkString(", ")}] do not fit SagaShape")
    require(outlets.size == 2, s"proposed outlets [${outlets.mkString(", ")}] do not fit SagaShape")

    //noinspection ZeroIndexToHead
    SagaStageShape(
      inlets(0).asInstanceOf[Inlet[Try[In]]],
      outlets(0).asInstanceOf[Outlet[Try[Out]]],
      inlets(1).asInstanceOf[Inlet[Option[Throwable]]],
      outlets(1).asInstanceOf[Outlet[Option[Throwable]]])
  }
}
