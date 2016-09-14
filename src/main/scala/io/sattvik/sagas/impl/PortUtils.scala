package io.sattvik.sagas.impl

import akka.stream.stage.{GraphStageLogic, InHandler}
import akka.stream.{Inlet, Outlet}

trait PortUtils { this: GraphStageLogic ⇒
  sealed trait PortStatus
  object PortStatus {
    def apply(i: Inlet[_]): InletStatus =
      (isAvailable(i), hasBeenPulled(i), isClosed(i)) match {
        case (true,  false, false) ⇒ Pushed
        case (false, false, false) ⇒ PushedEmpty
        case (true,  false, true)  ⇒ Closed
        case (false, true,  false) ⇒ Pulled
        case (false, false, true)  ⇒ ClosedEmpty
        case _ ⇒ throw new IllegalStateException("Bad inlet status.")
      }

    def apply(o: Outlet[_]): OutletStatus =
      (isAvailable(o), isClosed(o)) match {
        case (true,  false) ⇒ Pulled
        case (false, false) ⇒ Pushed
        case (false, true)  ⇒ Closed
        case _ ⇒ throw new IllegalStateException("Bad outlet status.")
      }
  }
  sealed trait InletStatus extends PortStatus {
    def apply(inlet: Inlet[_]): Boolean = PortStatus(inlet) == this
  }
  sealed trait OutletStatus extends PortStatus {
    def apply(outlet: Outlet[_]): Boolean = PortStatus(outlet) == this
  }
  case object Pushed extends InletStatus with OutletStatus
  case object Pulled extends InletStatus with OutletStatus
  case object Closed extends InletStatus with OutletStatus
  case object PushedEmpty extends InletStatus
  case object ClosedEmpty extends InletStatus

  protected def assertStatus(i: Inlet[_], inletStatuses: InletStatus*): Unit = {
    val s = PortStatus(i)
    assert(inletStatuses.toSet(s),
      s"$i is $s but should be one of: ${inletStatuses.mkString(", ")}"
    )
  }

  protected def assertStatus(o: Outlet[_], outletStatuses: OutletStatus*): Unit = {
    val s = PortStatus(o)
    assert(outletStatuses.toSet(s),
      s"$o is $s but should be one of: ${outletStatuses.mkString(", ")}"
    )
  }
}
