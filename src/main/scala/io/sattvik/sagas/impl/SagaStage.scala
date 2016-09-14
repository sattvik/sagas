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
        override def onPush(): Unit = state.onInPush()
        override def onUpstreamFinish(): Unit = state.onInFinished()
        override def onUpstreamFailure(ex: Throwable): Unit = state.onInFailed(ex)
      })

      setHandler(forwardIn, new OutHandler {
        override def onPull(): Unit = state.onForwardInPull()
        override def onDownstreamFinish(): Unit = state.onForwardInFinished()
      })

      setHandler(forwardOut, new InHandler {
        override def onPush(): Unit = state.onForwardOutPush()
        override def onUpstreamFinish(): Unit = state.onForwardOutFinished()
        override def onUpstreamFailure(ex: Throwable): Unit = state.onForwardOutFailed(ex)
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = state.onOutPull()
        override def onDownstreamFinish(): Unit = state.onOutFinished()
      })

      setHandler(downstreamRollback, new InHandler {
        override def onPush(): Unit = state.onDownstreamRollbackPush()
        override def onUpstreamFinish(): Unit = state.onDownstreamRollbackFinished()
        override def onUpstreamFailure(ex: Throwable): Unit = state.onDownstreamRollbackFailed(ex)
      })

      setHandler(rollbackIn, new OutHandler {
        override def onPull(): Unit = state.onRollbackInPull()
        override def onDownstreamFinish(): Unit = state.onRollbackInFinished()
      })

      setHandler(rollbackOut, new InHandler {
        override def onPush(): Unit = state.onRollbackOutPush()
        override def onUpstreamFinish(): Unit = state.onRollbackOutFinished()
        override def onUpstreamFailure(ex: Throwable): Unit = state.onRollbackOutFailed(ex)
      })

      setHandler(upstreamRollback, new OutHandler {
        override def onPull(): Unit = state.onUpstreamRollbackPull()
        override def onDownstreamFinish(): Unit = state.onUpstreamRollbackFinished()
      })

      private sealed trait State {
        def onInPush(): Unit = notAllowed("onInPush")
        def onInFinished(): Unit = notAllowed("onInFinished")
        def onInFailed(ex: Throwable): Unit = notAllowed("onInFailed")

        def onForwardInPull(): Unit = notAllowed("onForwardInPull")
        def onForwardInFinished(): Unit = notAllowed("onForwardInFinished")

        def onForwardOutPush(): Unit = notAllowed("onForwardOutPush")
        def onForwardOutFinished(): Unit = notAllowed("onForwardOutFinished")
        def onForwardOutFailed(ex: Throwable): Unit = notAllowed("onForwardOutFailed")

        def onOutPull(): Unit = notAllowed("onOutPull")
        def onOutFinished(): Unit = notAllowed("onOutFinished")

        def onDownstreamRollbackPush(): Unit = notAllowed("onDownstreamRollbackPush")
        def onDownstreamRollbackFinished(): Unit = notAllowed("onDownstreamRollbackFinished")
        def onDownstreamRollbackFailed(ex: Throwable): Unit = notAllowed("onDownstreamRollbackFailed")

        def onRollbackInPull(): Unit = notAllowed("onRollbackInPull")
        def onRollbackInFinished(): Unit = notAllowed("onRollbackInFinished")

        def onRollbackOutPush(): Unit = notAllowed("onRollbackOutPush")
        def onRollbackOutFinished(): Unit = notAllowed("onRollbackOutFinished")
        def onRollbackOutFailed(ex: Throwable): Unit = notAllowed("onRollbackOutFailed")

        def onUpstreamRollbackPull(): Unit = notAllowed("onUpstreamRollbackPull")
        def onUpstreamRollbackFinished(): Unit = notAllowed("onUpstreamRollbackFinished")

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

        override def onOutPull(): Unit = {
          assertStatus(in, PushedEmpty)
          assertStatus(upstreamRollback, Pushed)

          pull(forwardOut)
          transition(GettingInput, "onOutPull")
        } ensuring Pulled(forwardOut)

        override def onOutFinished(): Unit = {
          assertStatus(in, PushedEmpty)
          assertStatus(upstreamRollback, Closed)
          assertInvariants()

          cancel(rollbackOut)
          transition(ShuttingDown, "onOutFinished")
        } ensuring ClosedEmpty(rollbackOut)

        override def onInFinished(): Unit = {
          assertStatus(in, ClosedEmpty)
          assertStatus(upstreamRollback, Pushed)
          assertInvariants()

          complete(forwardIn)
          transition(ShuttingDown, "onInFinished")
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

        override def onForwardInPull(): Unit = {
          assertStatus(in, PushedEmpty)
          assertInvariants()

          pull(in)
          transition(this, "onForwardInPull")
        } ensuring Pulled(in)

        override def onInPush(): Unit = {
          assertStatus(in, Pushed)
          assertInvariants()

          val item = grab(in)
          push(forwardIn, item)
          transition(DoingWork(item), s"onInPush($item)")
        } ensuring Pushed(forwardIn)

        override def onUpstreamRollbackPull(): Unit = {
          assertStatus(in, PushedEmpty, Pulled)
          assertStatus(upstreamRollback, Pulled)

          // nothing to do here.  We do not pull downstream until we have
          // finished processing an input
          transition(this, "onUpstreamRollbackPull")
        }

        override def onInFinished(): Unit = {
          assertStatus(in, ClosedEmpty)
          assertInvariants()

          complete(forwardIn)
          transition(ShuttingDown, "onInFinished")
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

        override def onUpstreamRollbackPull(): Unit = {
          assertStatus(in, PushedEmpty)
          assertStatus(forwardIn, Pushed)
          assertStatus(forwardOut, Pulled)
          assertStatus(out, Pulled)

          assertStatus(downstreamRollback, PushedEmpty)
          assertStatus(rollbackIn, Pushed)
          assertStatus(rollbackOut, PushedEmpty)
          assertStatus(upstreamRollback, Pulled)

          transition(this, s"onUpstreamRollbackPull()")
        }

        /** Occurs when the flow fails. */
        override def onForwardInFinished(): Unit = {
          transition(FlowIsFailing(None), s"onForwardInFinished()")
        }

        /** Occurs when the flow fails. */
        override def onForwardOutFailed(ex: Throwable): Unit = {
          transition(FlowIsFailing(Some(ex)), s"onForwardOutFailed($ex)")
        }
      }

      private case class FlowIsFailing(ex: Option[Throwable]) extends State {
        override def onForwardInFinished(): Unit = {
          if (Pulled(upstreamRollback) && ClosedEmpty(forwardOut)) {
            push(upstreamRollback, ex)
            transition(InvokingUpstreamRollbacks(ex.get), s"onForwardInFinished()")
          } else {
            transition(this, "onForwardInFinished")
          }
          logState()
        }

        override def onForwardOutFailed(ex: Throwable): Unit = {
          if (Pulled(upstreamRollback) && Closed(forwardIn)) {
            push(upstreamRollback, Some(ex))
            transition(InvokingUpstreamRollbacks(ex), s"onForwardOutFailed($ex)")
          } else {
            transition(FlowIsFailing(Some(ex)), "onForwardOutFinished")
          }
          logState()
        }

        override def onUpstreamRollbackPull(): Unit = {
          if (ex.isDefined && Closed(forwardIn) && ClosedEmpty(forwardOut)) {
            push(upstreamRollback, ex)
            transition(InvokingUpstreamRollbacks(ex.get), s"onUpstreamRollbackPull()")
          } else {
            transition(this, "onUpstreamRollbackPull")
          }
          logState()
        }
      }

      private case class InvokingUpstreamRollbacks(ex: Throwable) extends State {
        override def onUpstreamRollbackFinished(): Unit = {
          cancel(rollbackOut)
          transition(this, "onUpstreamRollbackFinished")
        }

        override def onRollbackInFinished(): Unit = {
          cancel(downstreamRollback)
          transition(this, "onRollbackInFinished")
        }

        override def onOutFinished(): Unit = {
          cancel(in)
          transition(Failed(ex), "onOutFinished")
        }
      }

      private case class Failed(ex: Throwable) extends State

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

        override def onUpstreamRollbackPull(): Unit = {
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
        override def onUpstreamRollbackPull(): Unit = {
          push(upstreamRollback, maybeError)
          transition(Idle, "onUpstreamRollbackPull")
        }
      }

      private case object ShuttingDown extends State {
        override def onForwardOutFinished(): Unit = {
          assertStatus(in, ClosedEmpty)
          assertStatus(forwardIn, Closed)
          assertStatus(forwardOut, ClosedEmpty)
          assertStatus(out, Pushed, Pulled)

          assertStatus(downstreamRollback, PushedEmpty)
          assertStatus(rollbackIn, Pushed)
          assertStatus(rollbackOut, PushedEmpty)
          assertStatus(upstreamRollback, Pushed)

          complete(out)
          transition(this, "onForwardOutFinished")
        } ensuring Closed(out)

        override def onDownstreamRollbackFinished(): Unit = {
          assertStatus(in, ClosedEmpty)
          assertStatus(forwardIn, Closed)
          assertStatus(forwardOut, ClosedEmpty)
          assertStatus(out, Closed)

          assertStatus(downstreamRollback, ClosedEmpty)
          assertStatus(rollbackIn, Pushed)
          assertStatus(rollbackOut, PushedEmpty)
          assertStatus(upstreamRollback, Pushed)

          complete(rollbackIn)
          transition(this, "onDownstreamRollbackFinished")
        } ensuring Closed(rollbackIn)

        override def onRollbackOutFinished(): Unit = {
          assertStatus(in, ClosedEmpty)
          assertStatus(forwardIn, Closed)
          assertStatus(forwardOut, ClosedEmpty)
          assertStatus(out, Closed)

          assertStatus(downstreamRollback, ClosedEmpty)
          assertStatus(rollbackIn, Closed)
          assertStatus(rollbackOut, ClosedEmpty)
          assertStatus(upstreamRollback, Pushed)

          complete(upstreamRollback)
          transition(ShutDown, "onRollbackOutFinished")
        } ensuring Closed(upstreamRollback)

        override def onRollbackInFinished(): Unit = {
          assertStatus(in, PushedEmpty)
          assertStatus(forwardIn, Pushed)
          assertStatus(forwardOut, PushedEmpty)
          assertStatus(out, Pushed)

          assertStatus(downstreamRollback, PushedEmpty)
          assertStatus(rollbackIn, Closed)
          assertStatus(rollbackOut, ClosedEmpty)
          assertStatus(upstreamRollback, Closed)

          cancel(downstreamRollback)
          transition(this, "onRollbackInFinished")
        } ensuring ClosedEmpty(downstreamRollback)

        override def onOutFinished(): Unit = {
          assertStatus(in, PushedEmpty)
          assertStatus(forwardIn, Pushed)
          assertStatus(forwardOut, PushedEmpty)
          assertStatus(out, Closed)

          assertStatus(downstreamRollback, ClosedEmpty)
          assertStatus(rollbackIn, Closed)
          assertStatus(rollbackOut, ClosedEmpty)
          assertStatus(upstreamRollback, Closed)

          cancel(forwardOut)
          transition(this, "onOutFinished")
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
