package io.sattvik.sagas

import io.sattvik.sagas.impl.DefaultSagaFuture

import scala.concurrent.{ExecutionContext, Future}

trait SagaFuture[+T] {
  def map[S](f: T ⇒ S, rb: S ⇒ () ⇒ Future[Unit])
            (implicit executionContext: ExecutionContext): SagaFuture[S]

  def flatMap[S](f: ((T, () ⇒ Future[Unit])) ⇒ SagaFuture[S])
                (implicit executionContext: ExecutionContext): SagaFuture[S]
}

object SagaFuture {
  def apply[T](forward: Future[T],
               createRollback: T ⇒ () ⇒ Future[Unit])
              (implicit executor: ExecutionContext): SagaFuture[T] = {
    val forwardValueWithRollback =
      forward.map(t ⇒ (t, createRollback(t)))
    new DefaultSagaFuture[T](forwardValueWithRollback)
  }
}
