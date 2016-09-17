package io.sattvik.sagas

import org.scalatest.AsyncFreeSpec

import scala.concurrent.Future
import org.scalatest.Matchers._

class SagaFutureSpec extends AsyncFreeSpec {
  "a SagaFuture" - {
    "can do FizzBuzz" in {
      SagaFuture(
        Future.successful(9),
        { _: Int ⇒ () ⇒ Future.successful(()) }
      )
        .map(
          { n: Int ⇒
            if (n % 15 == 0) throw FizzBuzz(n) else n
          },
          { _: Int ⇒ { () ⇒ Future {
            println("rollback fizzbuzz")
          }}})
        .map(
          { n: Int ⇒
            if (n % 5 == 0) throw Buzz(n) else n
          },
          { _: Int ⇒ { () ⇒ Future {
            println("rollback buzz")
          }}})
        .map(
          { n: Int ⇒
            if (n % 3 == 0) throw Fizz(n) else n
          },
          { _: Int ⇒ { () ⇒ Future {
            println("rollback fizz")
          }}})
        .future.map { x ⇒
        x shouldBe 1
      }
    }

  }

  case class Fizz(n: Int) extends Exception
  case class Buzz(n: Int) extends Exception
  case class FizzBuzz(n: Int) extends Exception
}
