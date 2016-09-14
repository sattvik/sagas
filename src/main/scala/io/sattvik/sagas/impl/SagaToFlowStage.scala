package io.sattvik.sagas.impl

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, Inlet, Outlet}
import com.typesafe.scalalogging.StrictLogging

import scala.annotation.unchecked.uncheckedVariance

private[sagas] class SagaToFlowStage[-In,+Out] extends GraphStage[SagaToFlowShape[In @uncheckedVariance, Out @uncheckedVariance]] with StrictLogging {
  private val in: Inlet[In @uncheckedVariance] = Inlet("SagaToFlowStage.in")
  private val out: Outlet[Out @uncheckedVariance] = Outlet("SagaToFlowStage.out")
  private val intoFlow: Outlet[In @uncheckedVariance] = Outlet("SagaToFlowStage.intoFlow")
  private val fromFlow: Inlet[Out @uncheckedVariance] = Inlet("SagaToFlowStage.fromFlow")
  private val intoRollback: Outlet[Option[Throwable]] = Outlet("SagaToFlowShape.intoRollback")
  private val fromRollback: Inlet[Option[Throwable]] = Inlet("SagaToFlowShape.fromRollback")

  override val shape: SagaToFlowShape[In, Out] =
    SagaToFlowShape(in, out, intoFlow, fromFlow, intoRollback, fromRollback)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with PortUtils {
      /** The name of the current flow. */
      private val name = inheritedAttributes.getFirst[Attributes.Name](Attributes.Name("unnamed")).n
      /** Contains the current state of the stage. */
      private var state: State = Idle

      setHandler(in, new InHandler {
        override def onPush(): Unit = state.onGotInput()
        override def onUpstreamFinish(): Unit = state.onUpstreamCompleted()
        override def onUpstreamFailure(ex: Throwable): Unit = state.onInFailed(ex)
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = state.onDownstreamDemand()
        override def onDownstreamFinish(): Unit = state.onOutFinished()
      })

      setHandler(intoFlow, new OutHandler {
        override def onPull(): Unit = state.onFlowIsReady()
        override def onDownstreamFinish(): Unit = state.onFlowCancelled()
      })

      setHandler(fromFlow, new InHandler {
        override def onPush(): Unit = state.onFromFlowPush()
        override def onUpstreamFinish(): Unit = state.onFlowCompleted()
        override def onUpstreamFailure(ex: Throwable): Unit = state.onFromFlowFailed(ex)
      })

      setHandler(intoRollback, new OutHandler {
        override def onPull(): Unit = state.onIntoRollbackPull()
        override def onDownstreamFinish(): Unit = state.onRollbackFlowCancelled()
      })

      setHandler(fromRollback, new InHandler {
        override def onPush(): Unit = state.onFromRollbackPush()
        override def onUpstreamFinish(): Unit = state.onRollbackFlowCompleted()
        override def onUpstreamFailure(ex: Throwable): Unit = state.onFromRollbackFailed(ex)
      })

      private sealed trait State {
        def onGotInput(): Unit = notAllowed("onGotInput")
        def onUpstreamCompleted(): Unit = notAllowed("onUpstreamCompleted")
        def onInFailed(ex: Throwable): Unit = notAllowed("onInFailed")

        def onFlowIsReady(): Unit = notAllowed("onFlowIsReady")
        def onFlowCancelled(): Unit = notAllowed("onFlowCancelled")

        def onFromFlowPush(): Unit = notAllowed("onFromFlowPush")
        def onFlowCompleted(): Unit = notAllowed("onFlowCompleted")
        def onFromFlowFailed(ex: Throwable): Unit = notAllowed("onFromFlowFailed")

        def onDownstreamDemand(): Unit = notAllowed("onDownstreamDemand")
        def onOutFinished(): Unit = notAllowed("onOutFinished")

        def onIntoRollbackPull(): Unit = notAllowed("onIntoRollbackPull")
        def onRollbackFlowCancelled(): Unit = notAllowed("onRollbackFlowCancelled")

        def onFromRollbackPush(): Unit = notAllowed("onFromRollbackPush")
        def onRollbackFlowCompleted(): Unit = notAllowed("onRollbackFlowCompleted")
        def onFromRollbackFailed(ex: Throwable): Unit = notAllowed("onFromRollbackFailed")

        protected def assertInvariants(): Unit = {}

        private def notAllowed(method: String): Unit = {
          val msg = s"$this($name).$method: not allowed in this state"
          val cause = new IllegalStateException(msg)
          logger.error(msg)
          logState()
          failStage(cause)
        }
      }

      private case object Idle extends State {
        override def assertInvariants(): Unit = {
          assertStatus(intoFlow, Pushed)
          assertStatus(fromFlow, PushedEmpty)

          assertStatus(intoRollback, Pushed)
          assertStatus(fromRollback, PushedEmpty)
        }

        override def onDownstreamDemand(): Unit = {
          assertInvariants()
          assertStatus(in, PushedEmpty)
          assertStatus(out, Pulled)

          pull(fromFlow)
          transition(GettingInput, "onDownstreamDemand")
        } ensuring(PortStatus(fromFlow) == Pulled)

        override def onUpstreamCompleted(): Unit = {
          assertInvariants()
          assertStatus(in, ClosedEmpty)
          assertStatus(out, Pushed)

          complete(intoFlow)
          transition(ShuttingDown, "onUpstreamCompleted")
        } ensuring Closed(intoFlow)

        override def onOutFinished(): Unit = {
          assertInvariants()
          assertStatus(in, PushedEmpty)
          assertStatus(out, Closed)

          cancel(fromRollback)
          transition(ShuttingDown, "onOutFinished")
        }
      }

      private case object GettingInput extends State {
        override protected def assertInvariants(): Unit = {
          assertStatus(intoFlow, Pulled)
          assertStatus(fromFlow, Pulled)
          assertStatus(out, Pulled)

          assertStatus(intoRollback, Pushed)
          assertStatus(fromRollback, PushedEmpty)
        }

        override def onFlowIsReady(): Unit = {
          assertStatus(in, PushedEmpty)
          assertInvariants()

          pull(in)
          transition(GettingInput, "onFlowIsReady")
        } ensuring Pulled(in)

        override def onGotInput(): Unit = {
          assertStatus(in, Pushed)
          assertInvariants()

          val input = grab(in)
          push(intoFlow, input)
          pull(fromRollback)
          transition(ExecutingFlow(input), s"onGotInput($input)")
        } ensuring PushedEmpty(in) && Pulled(fromRollback) && Pushed(intoFlow)

        override def onUpstreamCompleted(): Unit = {
          assertStatus(in, ClosedEmpty)

          complete(intoFlow)
          transition(ShuttingDown, "onUpstreamComplteted")
        } ensuring Closed(intoFlow)
      }

      private case class ExecutingFlow(item: In) extends State {
        override def onUpstreamCompleted(): Unit = {
          assertStatus(in, ClosedEmpty)
          assertStatus(intoFlow, Pushed)
          assertStatus(fromFlow, Pulled)
          assertStatus(out, Pulled)

          assertStatus(intoRollback, Pushed, Pulled)
          assertStatus(fromRollback, Pulled)

          // We will need to wait for the flow to finish before closing
        }

        override def onFromFlowPush(): Unit = {
          assertStatus(in, PushedEmpty, ClosedEmpty)
          assertStatus(intoFlow, Pushed)
          assertStatus(fromFlow, Pushed)
          assertStatus(out, Pulled)

          assertStatus(intoRollback, Pushed, Pulled)
          assertStatus(fromRollback, Pulled)

          val result = grab(fromFlow)
          if (Pulled(intoRollback)) {
            push(intoRollback, None)
          }
          transition(AwaitingRollbackSuccess(result), s"onFromFlowPush($result)")
        } ensuring(PushedEmpty(fromFlow) && Pushed(intoRollback))

        override def onIntoRollbackPull(): Unit = {
          assertStatus(in, PushedEmpty, ClosedEmpty)
          assertStatus(intoFlow, Pushed)
          assertStatus(fromFlow, Pulled, Pushed)
          assertStatus(out, Pulled)

          assertStatus(intoRollback, Pulled)
          assertStatus(fromRollback, Pulled)
        }
      }

      private case class AwaitingRollbackSuccess(result: Out) extends State {
        override def assertInvariants(): Unit = {
          assertStatus(in, PushedEmpty, ClosedEmpty)
          assertStatus(intoFlow, Pushed)
          assertStatus(fromFlow, PushedEmpty)
          assertStatus(out, Pulled)
        }

        override def onIntoRollbackPull(): Unit = {
          assertStatus(intoRollback, Pulled)
          assertStatus(fromRollback, Pulled)
          assertInvariants()

          push(intoRollback, None)

          transition(this, "onIntoRollbackPull")
        } ensuring Pushed(intoRollback)

        override def onFromRollbackPush(): Unit = {
          assertStatus(intoRollback, Pushed)
          assertStatus(fromRollback, Pushed)
          assertInvariants()

          grab(fromRollback)
          push(out, result)

          if (ClosedEmpty(in)) {
            complete(intoFlow)
            transition(ShuttingDown, "onFromRollbackPush()")
          } else {
            transition(Idle, "onFromRollbackPush()")
          }
        } ensuring PushedEmpty(fromRollback) && Pushed(out)

        override def onUpstreamCompleted(): Unit =
          transition(this, "onUpstreamCompleted")
      }

      private case object ShuttingDown extends State {
        override def onDownstreamDemand(): Unit = {
          transition(this, "onDownstreamDemand")
        }

        override def onFlowCompleted(): Unit = {
          assertStatus(in, ClosedEmpty)
          assertStatus(intoFlow, Closed)
          assertStatus(fromFlow, ClosedEmpty)
          assertStatus(out, Pulled, Pushed)

          assertStatus(intoRollback, Pushed)
          assertStatus(fromRollback, PushedEmpty)

          complete(intoRollback)
          transition(this, "onFlowCompleted")
        } ensuring Closed(intoRollback)

        override def onRollbackFlowCompleted(): Unit = {
          assertStatus(in, ClosedEmpty)
          assertStatus(intoFlow, Closed)
          assertStatus(fromFlow, ClosedEmpty)
          assertStatus(out, Pulled, Pushed)

          assertStatus(intoRollback, Closed)
          assertStatus(fromRollback, ClosedEmpty)

          complete(out)
          transition(ShutDown, "onRollbackFlowCompleted")
        } ensuring Closed(out)

        override def onRollbackFlowCancelled(): Unit = {
          assertStatus(in, PushedEmpty)
          assertStatus(intoFlow, Pushed)
          assertStatus(fromFlow, PushedEmpty)
          assertStatus(out, Closed)

          assertStatus(intoRollback, Closed)
          assertStatus(fromRollback, ClosedEmpty)

          cancel(fromFlow)
          transition(this, "onRollbackFlowCancelled")
        } ensuring ClosedEmpty(fromFlow)

        override def onFlowCancelled(): Unit = {
          assertStatus(in, PushedEmpty)
          assertStatus(intoFlow, Closed)
          assertStatus(fromFlow, ClosedEmpty)
          assertStatus(out, Closed)

          assertStatus(intoRollback, Closed)
          assertStatus(fromRollback, ClosedEmpty)

          cancel(in)
          transition(ShutDown, "onFlowCancelled")
        } ensuring ClosedEmpty(in)
      }

      private case object ShutDown extends State {
      }

      @inline private def transition(nextState: State, method: String): Unit = {
        logger.trace(s"$state($name).$method → $nextState")
        state = nextState
      }

      private def logState(): Unit = {
        logger.trace(s"Current state for $state($name)")
        logger.trace(s"  in: ${PortStatus(in)}")
        logger.trace(s"  out: ${PortStatus(out)}")
        logger.trace(s"  intoFlow: ${PortStatus(intoFlow)}")
        logger.trace(s"  fromFlow: ${PortStatus(fromFlow)}")
        logger.trace(s"  intoRollback: ${PortStatus(intoRollback)}")
        logger.trace(s"  fromRollback: ${PortStatus(fromRollback)}")
      }
    }
}
