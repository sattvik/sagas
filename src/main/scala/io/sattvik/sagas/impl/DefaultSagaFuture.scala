package io.sattvik.sagas.impl

import io.sattvik.sagas.SagaFuture

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

private[sagas] class DefaultSagaFuture[+T](override val future: Future[T],
                                           withRollback: Future[(T, () ⇒ Future[Unit])]) extends SagaFuture[T] {
  override def map[S](f: (T) ⇒ S, rb: S ⇒ () ⇒ Future[Unit])
                     (implicit executionContext: ExecutionContext): SagaFuture[S] = {
    val result = Promise[S]()
    val resultWithRollback = Promise[(S, () ⇒ Future[Unit])]
    withRollback.onComplete {
      case Success((t, rollbackT)) ⇒
        try {
          val s = f(t)
          val fullRollback: () ⇒ Future[Unit] = { () ⇒
            rb(s)().flatMap(_ ⇒ rollbackT())
          }
          result.success(s)
          resultWithRollback.success((s, fullRollback))
        } catch {
          case NonFatal(e) ⇒
            val rollback = rollbackT()
            val asResult = rollback.flatMap(_ ⇒ Future.failed[S](e))
            val asResultWithRollback = rollback.flatMap(_ ⇒ Future.failed[(S, () ⇒ Future[Unit])](e))
            result.completeWith(asResult)
            resultWithRollback.completeWith(asResultWithRollback)
        }

      case Failure(ex) ⇒
        result.failure(ex)
        resultWithRollback.failure(ex)
    }
    new DefaultSagaFuture[S](result.future, resultWithRollback.future)
  }

  override def flatMap[S](f: (T, () ⇒ Future[Unit]) ⇒ SagaFuture[S])(implicit executionContext: ExecutionContext): SagaFuture[S] = ???
}
