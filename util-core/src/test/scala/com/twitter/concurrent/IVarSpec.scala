package com.twitter.concurrent

import java.util.concurrent.CountDownLatch
import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}
import org.scalatest.{WordSpec, Matchers}


class IVarSpec extends WordSpec with Matchers {
  "IVar" should {
    val iv = new IVar[Int]
    "invoke gets after value is set" in {
      var value: Option[Int] = None
      iv.get { v => value = Some(v) }
      value shouldEqual None

      iv.set(123) shouldBe true
      value shouldEqual Some(123)
    }

    "set value once" ignore {
      iv.set(123) shouldBe true
      iv() shouldEqual(123)
      iv.set(333) shouldBe false
      iv() shouldEqual(123)
    }

    "invoke multiple gets" in {
      var count = 0
      iv.get { _ => count += 1 }
      iv.get { _ => count += 1 }
      iv.set(123)
      count shouldEqual(2)
      iv.get { _ => count += 1 }
      count shouldEqual(3)
    }

    "chain properly" in {
      val order = new ArrayBuffer[Int]
      iv.chained.chained.get { _ => order += 3 }
      iv.chained.get { _ => order += 2 }
      iv.get { _ => order += 1 }
      iv.set(123)
      order should contain theSameElementsAs Seq(1, 2, 3)
    }

    "defer recursive gets (run a schedule)" in {
      var order = new ArrayBuffer[Int]
      def get(n: Int) {
        iv.get { _ =>
          if (n > 0) get(n - 1)
          order += n
        }
      }
      get(10)
      iv.set(123)
      order.toSeq shouldEqual(10 to 0 by -1 toSeq)
    }

    "remove waiters on unget" ignore {
      var didrun = false
      val k = { _: Int => didrun = true }
      iv.get(k)
      iv.unget(k)
      iv.set(1)
      didrun shouldBe false
    }
    
    "not remove another waiter on unget" in {
      var ran = false
      iv.get { _: Int => ran = true }
      iv.unget({_: Int => ()})
      iv.set(1)
      ran shouldBe true
    }

    "merge" should {
      val a, b, c = new IVar[Int]
      val events = new ArrayBuffer[String]
      "merges waiters" should {
        b.get { v => events += "b(%d)".format(v) }
        a.get { v => events += "a(%d)".format(v) }
        val expected = Seq("a(1)", "b(1)")
        a.merge(b)
        def test(x: IVar[Int], y: IVar[Int]) {
          x.set(1)
          events should contain theSameElementsAs(expected)
          x.isDefined shouldBe true
          y.isDefined shouldBe true

        }
        "a <- b" in test(a, b)
        "b <- a" in test(b, a)
      }

      "works transitively" in {
        val c = new IVar[Int]
        a.merge(b)
        b.merge(c)

        c.set(1)
        a.isDefined shouldBe true
        b.isDefined shouldBe true
        a() shouldEqual(1)
        b() shouldEqual(1)
      }

      "inherits an already defined value" in {
        a.set(1)
        b.merge(a)
        b.isDefined shouldBe true
        b() shouldEqual(1)
      }

      "does not fail if already defined" in {
        a.set(1)
        a.merge(b)
        b.isDefined shouldBe true
        b() shouldEqual(1)
      }

      "twoway merges" should {
        "succeed when values are equal" in {
          a.set(1)
          b.set(1)
          a.merge(b)
        }

        "succeed when values aren't equal, retaining values (it's a noop)" ignore {
          a.set(1)
          b.set(2)
          intercept[IllegalArgumentException] {
            a.merge(b)
          }
          a.poll shouldEqual Some(1)
          b.poll shouldEqual Some(2)
        }
      }

      "is idempotent" in {
        a.merge(b)
        a.merge(b)
        a.merge(b)
        b.set(123)
        a.isDefined shouldBe true
      }

      "performs path compression" in {
        var first = new IVar[Int]
        var i = new IVar[Int]
        i.set(1)
        first.merge(i)

        for (_ <- 0 until 100)
          (new IVar[Int]).merge(i)

        first.depth shouldEqual(0)
        i.depth shouldEqual(0)
      }

      "cycles" should {
        "deals with cycles in the done state" in {
          a.set(1)
          a.isDefined shouldBe true
          a.merge(a)
          a() shouldEqual(1)
        }

        "deals with shallow cycles in the waiting state" in {
          a.merge(a)
          a.set(1)
          a.isDefined shouldBe true
          a() shouldEqual(1)
        }

        "deals with simple indirect cycles" in {
          a.merge(b)
          b.merge(c)
          c.merge(a)
          b.set(1)
          a.isDefined shouldBe true
          b.isDefined shouldBe true
          c.isDefined shouldBe true
          a() shouldEqual(1)
          b() shouldEqual(1)
          c() shouldEqual(1)
        }
      }
    }

    "apply() recursively schedule (no deadlock)" in {
      @volatile var didit = false
      val t = new Thread("IVarSpec") {
        override def run() {
          val a, b = new IVar[Int]
          a.get { _ =>  // inside of the scheduler now
            a.get { _ =>
              b.set(1)  // this gets delayed
            }
            b.isDefined shouldBe false
            b()
            didit = true
          }

          a.set(1)
        }
      }
      t.start()
      t.join(500/*ms*/)
      didit shouldBe true
    }
  }
}
