package io.sattvik.sagas.impl

import akka.stream._
import akka.stream.stage._
import com.typesafe.scalalogging.StrictLogging

import scala.annotation.unchecked.uncheckedVariance

private[sagas] class SagaStage[-In,+Out] extends GraphStage[SagaStageShape[In @uncheckedVariance,Out @uncheckedVariance]] with StrictLogging {
  private val in: Inlet[In @uncheckedVariance] = Inlet("SagaStage.in")
  private val forwardIn: Outlet[In @uncheckedVariance] = Outlet("SagaStage.forwardIn")
  private val forwardOut: Inlet[Out @uncheckedVariance] = Inlet("SagaStage.forwardOut")
  private val out: Outlet[Out @uncheckedVariance] = Outlet("SagaStage.out")
  private val downstreamRollback: Inlet[Option[Throwable]] = Inlet("SagaStage.downstreamRollback")
  private val rollbackIn: Outlet[Out @uncheckedVariance] = Outlet("SagaStage.rollbackIn")
  private val rollbackOut: Inlet[Any] = Inlet("SagaStage.rollbackOut")
  private val upstreamRollback: Outlet[Option[Throwable]] = Outlet("SagaStage.upstreamRollback")

  override val shape: SagaStageShape[In, Out] =
    SagaStageShape(
      in, forwardIn, forwardOut, out,
      downstreamRollback, rollbackIn, rollbackOut, upstreamRollback)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with PortUtils {
      /** The name of the current flow. */
      private val name = inheritedAttributes.getFirst[Attributes.Name](Attributes.Name("unnamed")).n
      /** Contains the current state of the stage. */
      private var state: State = Idle

      @inline private def transition(nextState: State, method: String): Unit = {
        logger.trace(s"$state($name).$method → $nextState")
        state = nextState
      }

      setHandler(in, new InHandler {
        override def onPush(): Unit = state.onGotInput()
        override def onUpstreamFinish(): Unit = state.onUpstreamCompleted()
        override def onUpstreamFailure(ex: Throwable): Unit = state.onInFailed(ex)
      })

      setHandler(forwardIn, new OutHandler {
        override def onPull(): Unit = state.onForwardIsReady()
        override def onDownstreamFinish(): Unit = state.onForwardInFinished()
      })

      setHandler(forwardOut, new InHandler {
        override def onPush(): Unit = state.onForwardOutPush()
        override def onUpstreamFinish(): Unit = state.onForwardFlowCompleted()
        override def onUpstreamFailure(ex: Throwable): Unit = state.onForwardOutFailed(ex)
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = state.onDownstreamDemand()
        override def onDownstreamFinish(): Unit = state.onDownstreamCancelled()
      })

      setHandler(downstreamRollback, new InHandler {
        override def onPush(): Unit = state.onDownstreamRollbackPush()
        override def onUpstreamFinish(): Unit = state.onDownstreamCompleted()
        override def onUpstreamFailure(ex: Throwable): Unit = state.onDownstreamRollbackFailed(ex)
      })

      setHandler(rollbackIn, new OutHandler {
        override def onPull(): Unit = state.onRollbackInPull()
        override def onDownstreamFinish(): Unit = state.onRollbackFlowCancelled()
      })

      setHandler(rollbackOut, new InHandler {
        override def onPush(): Unit = state.onRollbackOutPush()
        override def onUpstreamFinish(): Unit = state.onRollbackFlowCompleted()
        override def onUpstreamFailure(ex: Throwable): Unit = state.onRollbackOutFailed(ex)
      })

      setHandler(upstreamRollback, new OutHandler {
        override def onPull(): Unit = state.onUpstreamIsReadyToRollback()
        override def onDownstreamFinish(): Unit = state.onDownstreamCancelled()
      })

      private sealed trait State {
        def onGotInput(): Unit = notAllowed("onGotInput")
        def onUpstreamCompleted(): Unit = notAllowed("onUpstreamCompleted")
        def onInFailed(ex: Throwable): Unit = notAllowed("onInFailed")

        def onForwardIsReady(): Unit = notAllowed("onForwardIsReady")
        def onForwardInFinished(): Unit = notAllowed("onForwardInFinished")

        def onForwardOutPush(): Unit = notAllowed("onForwardOutPush")
        def onForwardFlowCompleted(): Unit = notAllowed("onForwardFlowCompleted")
        def onForwardOutFailed(ex: Throwable): Unit = notAllowed("onForwardOutFailed")

        def onDownstreamDemand(): Unit = notAllowed("onDownstreamDemand")
        def onDownstreamDone(): Unit = notAllowed("onDownstreamDone")

        def onDownstreamRollbackPush(): Unit = notAllowed("onDownstreamRollbackPush")
        def onDownstreamCompleted(): Unit = notAllowed("onDownstreamCompleted")
        def onDownstreamRollbackFailed(ex: Throwable): Unit = notAllowed("onDownstreamRollbackFailed")

        def onRollbackInPull(): Unit = notAllowed("onRollbackInPull")
        def onRollbackFlowCancelled(): Unit = notAllowed("onRollbackFlowCancelled")

        def onRollbackOutPush(): Unit = notAllowed("onRollbackOutPush")
        def onRollbackFlowCompleted(): Unit = notAllowed("onRollbackFlowCompleted")
        def onRollbackOutFailed(ex: Throwable): Unit = notAllowed("onRollbackOutFailed")

        def onUpstreamIsReadyToRollback(): Unit = notAllowed("onUpstreamIsReadyToRollback")
        def onDownstreamCancelled(): Unit = notAllowed("onDownstreamCancelled")

        protected def assertInvariants(): Unit = {}

        private def notAllowed(method: String): Unit = {
          val msg = s"$this($name).$method: not allowed in this state"
          val cause = new IllegalStateException(msg)
          logState()
          logger.error(msg)
          failStage(cause)
        }
      }

      private case object Idle extends State {
        override def assertInvariants(): Unit = {
          assertStatus(forwardIn, Pushed)
          assertStatus(forwardOut, PushedEmpty)
          assertStatus(out, Pushed)

          assertStatus(downstreamRollback, PushedEmpty)
          assertStatus(rollbackIn, Pushed)
          assertStatus(rollbackOut, PushedEmpty)
        }

        override def onDownstreamDemand(): Unit = {
          assertStatus(in, PushedEmpty)
          assertStatus(upstreamRollback, Pushed)

          pull(forwardOut)
          transition(GettingInput, "onDownstreamDemand")
        } ensuring Pulled(forwardOut)

        override def onDownstreamCancelled(): Unit = {
          assertStatus(in, PushedEmpty)
          assertStatus(upstreamRollback, Closed)
          assertInvariants()

          cancel(rollbackOut)
          transition(ShuttingDown, "onDownstreamCancelled")
        } ensuring ClosedEmpty(rollbackOut)

        override def onUpstreamCompleted(): Unit = {
          assertStatus(in, ClosedEmpty)
          assertStatus(upstreamRollback, Pushed)
          assertInvariants()

          complete(forwardIn)
          transition(ShuttingDown, "onUpstreamCompleted")
        } ensuring Closed(forwardIn)
      }

      private case object GettingInput extends State {
        override def assertInvariants(): Unit = {
          assertStatus(forwardIn, Pulled)
          assertStatus(forwardOut, Pulled)
          assertStatus(out, Pulled)

          assertStatus(downstreamRollback, PushedEmpty)
          assertStatus(rollbackIn, Pushed)
          assertStatus(rollbackOut, PushedEmpty)
          assertStatus(upstreamRollback, Pushed, Pulled)
        }

        override def onForwardIsReady(): Unit = {
          assertStatus(in, PushedEmpty)
          assertInvariants()

          pull(in)
          transition(this, "onForwardIsReady")
        } ensuring Pulled(in)

        override def onGotInput(): Unit = {
          assertStatus(in, Pushed)
          assertInvariants()

          val item = grab(in)
          push(forwardIn, item)
          transition(DoingWork(item), s"onGotInput($item)")
        } ensuring Pushed(forwardIn)

        override def onUpstreamIsReadyToRollback(): Unit = {
          assertStatus(in, PushedEmpty, Pulled)
          assertStatus(upstreamRollback, Pulled)

          // nothing to do here.  We do not pull downstream until we have
          // finished processing an input
          transition(this, "onUpstreamIsReadyToRollback")
        }

        override def onUpstreamCompleted(): Unit = {
          assertStatus(in, ClosedEmpty)
          assertInvariants()

          complete(forwardIn)
          transition(ShuttingDown, "onUpstreamCompleted")
        } ensuring Closed(forwardIn)
      }

      private case class DoingWork(item: In) extends State {
        override def onForwardOutPush(): Unit = {
          assertStatus(in, PushedEmpty)
          assertStatus(forwardIn, Pushed)
          assertStatus(forwardOut, Pushed)
          assertStatus(out, Pulled)

          assertStatus(downstreamRollback, PushedEmpty)
          assertStatus(rollbackIn, Pushed)
          assertStatus(rollbackOut, PushedEmpty)
          assertStatus(upstreamRollback, Pushed, Pulled)

          val item = grab(forwardOut)
          push(out, item)
          pull(downstreamRollback)
          transition(AwaitingDownstreamResult(item), s"onForwardOutPush($item)")
        } ensuring(Pulled(downstreamRollback) && Pushed(out) && PushedEmpty(forwardOut))

        override def onUpstreamIsReadyToRollback(): Unit = {
          assertStatus(in, PushedEmpty)
          assertStatus(forwardIn, Pushed)
          assertStatus(forwardOut, Pulled)
          assertStatus(out, Pulled)

          assertStatus(downstreamRollback, PushedEmpty)
          assertStatus(rollbackIn, Pushed)
          assertStatus(rollbackOut, PushedEmpty)
          assertStatus(upstreamRollback, Pulled)

          transition(this, s"onUpstreamIsReadyToRollback()")
        }
      }

      private case class AwaitingDownstreamResult(item: Out) extends State {
        override def onDownstreamRollbackPush(): Unit = {
          assertStatus(in, PushedEmpty)
          assertStatus(forwardIn, Pushed)
          assertStatus(forwardOut, PushedEmpty)
          assertStatus(out, Pushed)

          assertStatus(downstreamRollback, Pushed)
          assertStatus(rollbackIn, Pushed)
          assertStatus(rollbackOut, PushedEmpty)
          assertStatus(upstreamRollback, Pulled, Pushed)

          val maybeError: Option[Throwable] = grab(downstreamRollback)
          maybeError match {
            case Some(ex) ⇒

            case None ⇒
              PortStatus(upstreamRollback) match {
                case Pulled ⇒
                  // upstream is ready, push the rollback and go idle
                  push(upstreamRollback, None)
                  transition(Idle, s"onDownstreamRollbackPush($maybeError)")
                case Pushed ⇒
                  transition(AwaitingUpstreamRollback(maybeError), s"onDownstreamRollbackPush($maybeError)")
                case Closed ⇒
                  failStage(new IllegalStateException(s"$upstreamRollback is Closed but should not be."))
              }
          }
        }

        override def onUpstreamIsReadyToRollback(): Unit = {
          assertStatus(in, PushedEmpty)
          assertStatus(forwardIn, Pushed)
          assertStatus(forwardOut, PushedEmpty)
          assertStatus(out, Pushed)

          assertStatus(downstreamRollback, Pulled)
          assertStatus(rollbackIn, Pushed)
          assertStatus(rollbackOut, PushedEmpty)
          assertStatus(upstreamRollback, Pulled)
        }
      }

      private case class AwaitingUpstreamRollback(maybeError: Option[Throwable]) extends State {
        override def onUpstreamIsReadyToRollback(): Unit = {
          push(upstreamRollback, maybeError)
          transition(Idle, "onUpstreamIsReadyToRollback")
        }
      }

      private case object ShuttingDown extends State {
        override def onForwardFlowCompleted(): Unit = {
          assertStatus(in, ClosedEmpty)
          assertStatus(forwardIn, Closed)
          assertStatus(forwardOut, ClosedEmpty)
          assertStatus(out, Pushed, Pulled)

          assertStatus(downstreamRollback, PushedEmpty)
          assertStatus(rollbackIn, Pushed)
          assertStatus(rollbackOut, PushedEmpty)
          assertStatus(upstreamRollback, Pushed)

          complete(out)
          transition(this, "onForwardFlowCompleted")
        } ensuring Closed(out)

        override def onDownstreamCompleted(): Unit = {
          assertStatus(in, ClosedEmpty)
          assertStatus(forwardIn, Closed)
          assertStatus(forwardOut, ClosedEmpty)
          assertStatus(out, Closed)

          assertStatus(downstreamRollback, ClosedEmpty)
          assertStatus(rollbackIn, Pushed)
          assertStatus(rollbackOut, PushedEmpty)
          assertStatus(upstreamRollback, Pushed)

          complete(rollbackIn)
          transition(this, "onDownstreamCompleted")
        } ensuring Closed(rollbackIn)

        override def onRollbackFlowCompleted(): Unit = {
          assertStatus(in, ClosedEmpty)
          assertStatus(forwardIn, Closed)
          assertStatus(forwardOut, ClosedEmpty)
          assertStatus(out, Closed)

          assertStatus(downstreamRollback, ClosedEmpty)
          assertStatus(rollbackIn, Closed)
          assertStatus(rollbackOut, ClosedEmpty)
          assertStatus(upstreamRollback, Pushed)

          complete(upstreamRollback)
          transition(ShutDown, "onRollbackFlowCompleted")
        } ensuring Closed(upstreamRollback)

        override def onRollbackFlowCancelled(): Unit = {
          assertStatus(in, PushedEmpty)
          assertStatus(forwardIn, Pushed)
          assertStatus(forwardOut, PushedEmpty)
          assertStatus(out, Pushed)

          assertStatus(downstreamRollback, PushedEmpty)
          assertStatus(rollbackIn, Closed)
          assertStatus(rollbackOut, ClosedEmpty)
          assertStatus(upstreamRollback, Closed)

          cancel(downstreamRollback)
          transition(this, "onRollbackFlowCancelled")
        } ensuring ClosedEmpty(downstreamRollback)

        override def onDownstreamCancelled(): Unit = {
          assertStatus(in, PushedEmpty)
          assertStatus(forwardIn, Pushed)
          assertStatus(forwardOut, PushedEmpty)
          assertStatus(out, Closed)

          assertStatus(downstreamRollback, ClosedEmpty)
          assertStatus(rollbackIn, Closed)
          assertStatus(rollbackOut, ClosedEmpty)
          assertStatus(upstreamRollback, Closed)

          cancel(forwardOut)
          transition(this, "onDownstreamCancelled")
        } ensuring ClosedEmpty(forwardOut)

        override def onForwardInFinished(): Unit = {
          assertStatus(in, PushedEmpty)
          assertStatus(forwardIn, Closed)
          assertStatus(forwardOut, ClosedEmpty)
          assertStatus(out, Closed)

          assertStatus(downstreamRollback, ClosedEmpty)
          assertStatus(rollbackIn, Closed)
          assertStatus(rollbackOut, ClosedEmpty)
          assertStatus(upstreamRollback, Closed)

          cancel(in)
          transition(ShutDown, "onForwardInFinished")
        } ensuring ClosedEmpty(in)
      }

      private case object ShutDown extends State

      private def logState(): Unit = {
        logger.trace(s"Current state for $state($name)")
        logger.trace(s"  Forward:")
        logger.trace(s"    in: ${PortStatus(in)}")
        logger.trace(s"    forwardIn: ${PortStatus(forwardIn)}")
        logger.trace(s"    forwardOut: ${PortStatus(forwardOut)}")
        logger.trace(s"    out: ${PortStatus(out)}")
        logger.trace(s"  Reverse:")
        logger.trace(s"    downstreamRollback: ${PortStatus(downstreamRollback)}")
        logger.trace(s"    rollbackIn: ${PortStatus(rollbackIn)}")
        logger.trace(s"    rollbackOut: ${PortStatus(rollbackOut)}")
        logger.trace(s"    upstreamRollback: ${PortStatus(upstreamRollback)}")
      }
    }
}
