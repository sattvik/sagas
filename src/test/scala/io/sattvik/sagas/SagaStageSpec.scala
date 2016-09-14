package io.sattvik.sagas

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.stream.{ActorMaterializer, ClosedShape}
import org.scalatest.{AsyncFreeSpec, FreeSpec}
import org.scalatest.Matchers._

import scala.concurrent.duration.DurationInt
import SagaFlow.Implicits._
import org.scalatest.prop.GeneratorDrivenPropertyChecks._
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalatest.concurrent.ScalaFutures._

import scala.util.{Failure, Success, Try}

class SagaStageSpec extends FreeSpec {
  implicit val system = ActorSystem("SagaStageSpec")
  implicit val ec = system.dispatcher
  implicit val m = ActorMaterializer()
  implicit val generatorDrivenConfig = PropertyCheckConfig(minSuccessful = 250, maxSize = 25)

  def keepAll[M1,M2,M3,M4](a: M1, b: M2, c: M3, d: M4): (M1, M2, M3, M4) = (a, b, c, d)

  val genTakeCount = Gen.choose(0, 1000)

  "a SagaStage" - {
    "with a single stage" - {
//      "consumes an empty source" in {
//        val (pub, sub) = SagaFlow.fromFlows(Flow[Int],Flow[Int]).toFlow.runWith(TestSource.probe[Int], TestSink.probe[Int])
//
//        pub.sendComplete()
//        sub.expectSubscription()
//        sub.expectComplete()
//      }
//
//      "consumes a single event with an upstream-initiated close" in {
//        val (pub, sub) = SagaFlow.fromFlows(Flow[Int],Flow[Int]).toFlow.runWith(TestSource.probe[Int], TestSink.probe[Int])
//
//        pub.sendNext(1)
//        pub.sendComplete()
//
//        sub.requestNext(1)
//        sub.expectComplete()
//      }
//
//      "consumes a single event with a downstream-initiated close" in {
//        val (pub, sub) = SagaFlow.fromFlows(Flow[Int],Flow[Int]).toFlow.runWith(TestSource.probe[Int], TestSink.probe[Int])
//
//        pub.sendNext(1)
//        sub.requestNext(1)
//        sub.cancel()
//        pub.expectCancellation()
//      }

      "consumes events" in {
        val saga = SagaFlow.fromFlows(Flow[Int],Flow[Int]).tryFlow

        forAll(
          arbitrary[List[Int]] → "inputs",
          genTakeCount → "numToTake"
        ) { (input, takeCount) ⇒
          val (_, result) = saga.take(takeCount).runWith(Source(input), Sink.seq)
          result.futureValue shouldBe input.take(takeCount).map(Success(_))
        }
      }

      "fails when the first stage fails" in {
        val boom = new Exception("BOOM!")
        val failing = Flow[Int].map(_ ⇒ throw boom)
        val (pub, sub) = SagaFlow.fromFlows(failing,Flow[Int]).tryFlow
            .runWith(TestSource.probe[Int], TestSink.probe[Try[Int]])

        pub.sendNext(1)
        sub.requestNext(Failure(boom))
      }

      "consumes events which end in a failure" in {
        val boom = new Exception("BOOM!")
        val saga = SagaFlow.fromFlows(Flow[Int].map(x ⇒ if (x == 0) throw boom else x),Flow[Int]).tryFlow
//        val saga = SagaFlow.fromFlows(Flow[Int],Flow[Int]).tryFlow

        forAll(Gen.listOf(Gen.choose(0,10)) → "inputs") { input ⇒
          val (_, result) = saga.runWith(Source(input), Sink.seq)

          println(result.futureValue)
          result.futureValue shouldBe input.collect {
            case 0 ⇒ Failure(boom)
            case x ⇒ Success(x)
          }
        }
      }

      "can do fizz-buzz" in {

        val fizzyOrBuzzy = Seq.newBuilder[Int]
        val fizzBuzzStage =
          SagaFlow.fromFlows(
            Flow[Int].map(n ⇒ if (n % 15 == 0) throw FizzBuzz(n) else n),
            Flow[Int].map(n ⇒ fizzyOrBuzzy += n)
          )

        val fizzy = Seq.newBuilder[Int]
        val buzzStage =
          SagaFlow.fromFlows(
            Flow[Int].map(n ⇒ if (n % 5 == 0) throw Buzz(n) else n),
            Flow[Int].map(n ⇒ fizzy += n)
          )

        val empty = Seq.newBuilder[Int]
        val fizzStage =
          SagaFlow.fromFlows(
            Flow[Int].map(n ⇒ if (n % 3 == 0) throw Fizz(n) else n),
            Flow[Int].map(n ⇒ empty += n)
          )

        val fizzBuzzSaga =
          fizzBuzzStage.atop(buzzStage).atop(fizzStage)

        val result =
          Source(1.to(15))
            .via(fizzBuzzSaga.tryFlow)
            .runWith(Sink.seq)

        result.futureValue shouldBe
          List(
            Success(1),
            Success(2),
            Failure(Fizz(3)),
            Success(4),
            Failure(Buzz(5)),
            Failure(Fizz(6)),
            Success(7),
            Success(8),
            Failure(Fizz(9)),
            Failure(Buzz(10)),
            Success(11),
            Failure(Fizz(12)),
            Success(13),
            Success(14),
            Failure(FizzBuzz(15))
          )

        empty.result() shouldBe List()
        fizzy.result() shouldBe List(3, 6, 9, 12)
        fizzyOrBuzzy.result() shouldBe List(3, 5, 6, 9, 10, 12)
      }
    }
  }

  case class Fizz(n: Int) extends Exception
  case class Buzz(n: Int) extends Exception
  case class FizzBuzz(n: Int) extends Exception
}
