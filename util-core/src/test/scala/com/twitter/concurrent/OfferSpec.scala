package com.twitter.concurrent


import org.scalatest.{WordSpec, Matchers}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers._

import com.twitter.util.{Future, Return, Promise, Await}
import com.twitter.util.{Time, MockTimer}
import com.twitter.conversions.time._
import scala.util.Random

class SimpleOffer[T](var futures: Stream[Future[Tx[T]]]) extends Offer[T] {
  def this(fut: Future[Tx[T]]) = this(Stream.continually(fut))
  def this(tx: Tx[T]) = this(Stream.continually(Future.value(tx)))

  def prepare() = {
    val next #:: rest = futures
    futures = rest
    next
  }
}

class OfferSpec extends WordSpec with Matchers with MockitoSugar {
  import Tx.{Commit, Abort}

  "Offer.map" should {
    // mockito can't spy on anonymous classes.
    val tx = mock[Tx[Int]]
    tx.ack() should equal (Future.value(Commit(123)))
    val offer = spy(new SimpleOffer(tx))

    val mapped = offer map { i => (i - 100).toString }

    "apply f in after Tx.ack()" in {
      val f = mapped.prepare() flatMap { tx => tx.ack() }
      f match {
        case Future(Return(Commit("23"))) => assert(true)
      }
    }
  }

  "Offer.choose" should {
    val pendingTxs = 0 until 3 map { _ => new Promise[Tx[Int]] }
    val offers = pendingTxs map { tx => spy(new SimpleOffer(tx)) }
    val offer = Offer.choose(offers:_*)

    "when a tx is already ready" in {
      val tx1 = mock[Tx[Int]]
      pendingTxs(1).setValue(tx1)

      "prepare() prepares all" in {
        offers foreach { of => verify(of, never()).prepare() }
        offer.prepare().isDefined shouldBe true
        offers foreach { of => verify(of).prepare() }
      }

      "select it" in {
        offer.prepare() match {
          case Future(Return(tx)) => assert(tx eq tx1)
        }
        verify(tx1, never()).ack()
        verify(tx1, never()).nack()
      }

      "nack losers" in {
        offer.prepare()
        for (i <- Seq(0, 2)) {
          val tx = mock[Tx[Int]]
          pendingTxs(i).setValue(tx)
          verify(tx).nack()
          verify(tx, never()).ack()
        }
      }
    }

    "when a tx is ready after prepare()" in {
      "select it" in {
        val tx = offer.prepare()
        tx.isDefined shouldBe false
        val tx0 = mock[Tx[Int]]
        pendingTxs(0).setValue(tx0)
        tx match {
          case Future(Return(tx)) => assert(tx eq tx0)
        }
      }

      "nack losers" in {
        offer.prepare()
        pendingTxs(0).setValue(mock[Tx[Int]])
        for (i <- Seq(1, 2)) {
          val tx = mock[Tx[Int]]
          pendingTxs(i).setValue(tx)
          verify(tx).nack()
          verify(tx, never()).ack()
        }
      }
    }

    "when all txs are ready" in {
      val txs = for (p <- pendingTxs) yield {
        val tx = mock[Tx[Int]]
        p.setValue(tx)
        tx
      }

      "shuffle winner" in Time.withTimeAt(Time.epoch) { tc =>
        val shuffledOffer = Offer.choose(new Random(Time.now.inNanoseconds), offers)
        val histo = new Array[Int](3)
        for (_ <- 0 until 1000) {
          for (tx <- shuffledOffer.prepare())
            histo(txs.indexOf(tx)) += 1
        }

        histo(0) shouldEqual(311)
        histo(1) shouldEqual(346)
        histo(2) shouldEqual(343)
      }

      "nack losers" in {
        offer.prepare() match {
          case Future(Return(tx)) =>
            for (loser <- txs if loser ne tx)
              verify(loser).nack()
            assert(true)
        }
      }
    }

    "work with 0 offers" in {
      val of = Offer.choose()
      of.sync().poll shouldEqual None
    }
  }

  "Offer.sync" should {
    "when Tx.prepare is immediately available" in {
      "when it commits" in {
        val txp = new Promise[Tx[Int]]
        val offer = spy(new SimpleOffer(txp))
        val tx = mock[Tx[Int]]
        tx.ack() should equal (Future.value(Commit(123)))
        txp.setValue(tx)
        offer.sync() match {
          case Future(Return(123)) => assert(true)
        }
        verify(tx).ack()
        verify(tx, never()).nack()
      }

      "retry when it aborts" in {
        val txps = new Promise[Tx[Int]] #:: new Promise[Tx[Int]] #:: Stream.empty
        val offer = spy(new SimpleOffer(txps))
        val badTx = mock[Tx[Int]]
        badTx.ack() should equal (Future.value(Abort))
        txps(0).setValue(badTx)

        val syncd = offer.sync()

        syncd.poll shouldEqual None
        verify(badTx).ack()
        verify(offer, times(2)).prepare()

        val okTx = mock[Tx[Int]]
        okTx.ack() should equal (Future.value(Commit(333)))
        txps(1).setValue(okTx)

        syncd.poll shouldEqual Some(Return(333))

        verify(okTx).ack()
        verify(offer, times(2)).prepare()
      }
    }

    "when Tx.prepare is delayed" in {
      "when it commits" in {
        val tx = mock[Tx[Int]]
        tx.ack() should equal (Future.value(Commit(123)))
        val offer = spy(new SimpleOffer(tx))

        offer.sync() match {
          case Future(Return(123)) => assert(true)
        }
        verify(tx).ack()
        verify(tx, never()).nack()
        verify(offer).prepare()
      }
    }
  }

  "Offer.const" should {
    "always provide the same result" in {
      val offer = Offer.const(123)

      offer.sync().poll shouldEqual Some(Return(123))
      offer.sync().poll shouldEqual Some(Return(123))
    }

    "evaluate argument for each prepare()" in {
      var i = 0
      val offer = Offer.const { i = i + 1; i }
      offer.sync().poll shouldEqual Some(Return(1))
      offer.sync().poll shouldEqual Some(Return(2))
    }
  }

  "Offer.orElse" should {
    "with const orElse" in {
      val txp = new Promise[Tx[Int]]
      val e0 = spy(new SimpleOffer(txp))

      "prepare orElse event when prepare isn't immediately available" in {
        val e1 = Offer.const(123)
        val offer = e0 orElse e1
        offer.sync().poll shouldEqual Some(Return(123))
        verify(e0).prepare()
        val tx = mock[Tx[Int]]
        txp.setValue(tx)
        verify(tx).nack()
        verify(tx, never()).ack()
      }

      "not prepare orElse event when the result is immediately available" in {
        val e1 = spy(new SimpleOffer(Stream.empty))
        val offer = e0 orElse e1
        val tx = mock[Tx[Int]]
        tx.ack() should equal (Future.value(Commit(321)))
        txp.setValue(tx)
        offer.sync().poll shouldEqual Some(Return(321))
        verify(e0).prepare()
        verify(tx).ack()
        verify(tx, never()).nack()
        verify(e1, never).prepare()
      }
    }

    "sync integration: when first transaction aborts" in {
      val tx2 = new Promise[Tx[Int]]
      val e0 = spy(new SimpleOffer(Future.value(Tx.aborted: Tx[Int]) #:: (tx2: Future[Tx[Int]]) #:: Stream.empty))
      val offer = e0 orElse Offer.const(123)

      "select first again if tx2 is ready" in {
        val tx = mock[Tx[Int]]
        tx.ack() should equal (Future.value(Commit(321)))
        tx2.setValue(tx)

        offer.sync().poll shouldEqual Some(Return(321))
        verify(e0, times(2)).prepare()
        verify(tx).ack()
        verify(tx, never()).nack()
      }

      "select alternative if not ready the second time" in {
        offer.sync().poll shouldEqual Some(Return(123))
        verify(e0, times(2)).prepare()

        val tx = mock[Tx[Int]]
        tx2.setValue(tx)
        verify(tx).nack()
      }
    }
  }

  "Offer.foreach" should {
    "synchronize on offers forever" in {
      val b = new Broker[Int]
      var count = 0
      b.recv foreach { _ => count += 1 }
      count shouldEqual(0)
      b.send(1).sync().isDefined shouldBe true
      count shouldEqual(1)
      b.send(1).sync().isDefined shouldBe true
      count shouldEqual(2)
    }
  }

  "Offer.timeout" should {
    "be available after timeout (prepare)" in Time.withTimeAt(Time.epoch) { tc =>
      implicit val timer = new MockTimer
      val e = Offer.timeout(10.seconds)
      e.prepare().isDefined shouldBe false
      tc.advance(9.seconds)
      timer.tick()
      e.prepare().isDefined shouldBe false
      tc.advance(1.second)
      timer.tick()
      e.prepare().isDefined shouldBe true
    }

    "cancel timer tasks when losing" in Time.withTimeAt(Time.epoch) { tc =>
      implicit val timer = new MockTimer
      val e10 = Offer.timeout(10.seconds) map { _ => 10 }
      val e5 = Offer.timeout(5.seconds) map { _ => 5 }

      val item = Offer.select(e5, e10)
      item.poll shouldEqual None
      timer.tasks should have size (2)
      timer.nCancelled shouldEqual(0)

      tc.advance(6.seconds)
      timer.tick()

      item.poll shouldEqual Some(Return(5))
      timer.tasks should have size (0)
      timer.nCancelled shouldEqual(1)
    }
  }

  "Integration" should {
    "select across multiple brokers" in {
      val b0 = new Broker[Int]
      val b1 = new Broker[String]

      val o = Offer.choose(
        b0.send(123) const { "put!" },
        b1.recv
      )

      val f = o.sync()
      f.isDefined shouldBe false
      b1.send("hey").sync().isDefined shouldBe true
      f.isDefined shouldBe true
      Await.result(f) shouldEqual("hey")

      val gf = b0.recv.sync()
      gf.isDefined shouldBe false
      val of = o.sync()
      of.isDefined shouldBe true
      Await.result(of) shouldEqual("put!")
      gf.isDefined shouldBe true
      Await.result(gf) shouldEqual(123)

      // syncing again fails.
      o.sync().isDefined shouldBe false
    }
  }
}
