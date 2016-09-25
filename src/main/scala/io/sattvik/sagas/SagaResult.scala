package io.sattvik.sagas

/** Designates whether a saga completed successfully or failed. */
sealed trait SagaResult

/** Indicates that the saga completed successfully. */
case object SagaSucceeded extends SagaResult

/** Indicates that the saga failed and that completed stages should be rolled
  * back.
  */
case object SagaFailed extends SagaResult

