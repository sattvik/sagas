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

class SagaStageSpec extends FreeSpec {
  implicit val system = ActorSystem("SagaStageSpec")
  implicit val m = ActorMaterializer()
  implicit val generatorDrivenConfig = PropertyCheckConfig(minSuccessful = 200, maxSize = 25, maxDiscarded = 1)

  def keepAll[M1,M2,M3,M4](a: M1, b: M2, c: M3, d: M4): (M1, M2, M3, M4) = (a, b, c, d)

  val genTakeCount = Gen.choose(0, 1000)

  "a SagaStage" - {
    "with a single stage" - {
      "consumes an empty source" in {
        val (pub, sub) = SagaFlow.fromFlows(Flow[Int],Flow[Int]).toFlow.runWith(TestSource.probe[Int], TestSink.probe[Int])

        pub.sendComplete()
        sub.expectSubscription()
        sub.expectComplete()
      }

      "consumes a single event with an upstream-initiated close" in {
        val (pub, sub) = SagaFlow.fromFlows(Flow[Int],Flow[Int]).toFlow.runWith(TestSource.probe[Int], TestSink.probe[Int])

        pub.sendNext(1)
        pub.sendComplete()

        sub.requestNext(1)
        sub.expectComplete()
      }

      "consumes a single event with a downstream-initiated close" in {
        val (pub, sub) = SagaFlow.fromFlows(Flow[Int],Flow[Int]).toFlow.runWith(TestSource.probe[Int], TestSink.probe[Int])

        pub.sendNext(1)
        sub.requestNext(1)
        sub.cancel()
        pub.expectCancellation()
      }

      "consumes events" in {
        val saga = SagaFlow.fromFlows(Flow[Int],Flow[Int]).toFlow

        forAll(
          arbitrary[List[Int]] → "inputs",
          genTakeCount → "numToTake"
        ) { (input, takeCount) ⇒
          val (_, result) = saga.take(takeCount).runWith(Source(input), Sink.seq)
          result.futureValue shouldBe input.take(takeCount)
        }
      }

      "handles a failed stage" ignore {
        val failing = Flow[Int].map(_ ⇒ throw new Exception("BOOOM!"))
        val (pub, sub) = SagaFlow.fromFlows(failing,Flow[Int]).toFlow.runWith(TestSource.probe[Int], TestSink.probe[Int])

        pub.sendNext(1)
        pub.sendComplete()

        sub.requestNext(1)
        sub.expectComplete()
      }
    }

    "with two stages" - {
      "consumes events" in {
        val stage1 = SagaFlow.fromFlows(Flow[Int],Flow[Int])
        val stage2 = SagaFlow.fromFlows(Flow[Int],Flow[Int])
        val saga = stage1.atop(stage2).toFlow

        forAll(
          arbitrary[List[Int]] → "inputs",
          genTakeCount → "numToTake"
        ) { (input, takeCount) ⇒
          val (_, result) = saga.take(takeCount).runWith(Source(input), Sink.seq)
          result.futureValue shouldBe input.take(takeCount)
        }
      }
    }
  }
}
