package com.twitter.util

import com.twitter.conversions.time._
import java.util.concurrent.{Future => JFuture}
import java.util.concurrent._

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.{Millis, Seconds, Span}

@RunWith(classOf[JUnitRunner])
class FuturePoolTest extends FunSuite with Eventually {

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(5, Millis)))

  test("FuturePool should dispatch to another thread") {
    val executor = Executors.newFixedThreadPool(1).asInstanceOf[ThreadPoolExecutor]
    val pool     = FuturePool(executor)

    val source = new Promise[Int]
    val result = pool { Await.result(source) } // simulate blocking call

    source.setValue(1)
    assert(Await.result(result) === 1)
  }

  test("Executor failing contains failures") {
    val badExecutor = new ScheduledThreadPoolExecutor(1) {
      override def submit(runnable: Runnable): JFuture[_] = {
        throw new RejectedExecutionException()
      }
    }

    val pool = FuturePool(badExecutor)

    val runCount = new atomic.AtomicInteger(0)

    val result1 = pool {
      runCount.incrementAndGet()
    }

    assert(runCount.get() === 0)
  }

  test("does not execute interrupted tasks") {
    val executor = Executors.newFixedThreadPool(1).asInstanceOf[ThreadPoolExecutor]
    val pool     = FuturePool(executor)

    val runCount = new atomic.AtomicInteger

    val source1 = new Promise[Int]
    val source2 = new Promise[Int]

    val result1 = pool { runCount.incrementAndGet(); Await.result(source1) }
    val result2 = pool { runCount.incrementAndGet(); Await.result(source2) }

    result2.raise(new Exception)
    source1.setValue(1)

    // The executor will run the task for result 2, but the wrapper
    // in FuturePool will throw away the work if the future
    // representing the outcome has already been interrupted,
    // and will set the result to a CancellationException
    eventually { assert(executor.getCompletedTaskCount === 2) }

    assert(runCount.get() === 1)
    assert(Await.result(result1)  === 1)
    intercept[CancellationException] { Await.result(result2) }
  }

  test("continue to run a task if it's interrupted while running") {
    val executor = Executors.newFixedThreadPool(1).asInstanceOf[ThreadPoolExecutor]
    val pool     = FuturePool(executor)

    val runCount = new atomic.AtomicInteger

    val source = new Promise[Int]

    val startedLatch = new CountDownLatch(1)
    val cancelledLatch = new CountDownLatch(1)

    val result: Future[Int] = pool {
      try {
        startedLatch.countDown()
        runCount.incrementAndGet()
        cancelledLatch.await()
        throw new RuntimeException()
      } finally {
        runCount.incrementAndGet()
      }
      runCount.get
    }

    startedLatch.await(50.milliseconds)
    result.raise(new Exception)
    cancelledLatch.countDown()

    eventually { assert(executor.getCompletedTaskCount === 1) }

    assert(runCount.get() === 2)
    intercept[RuntimeException] { Await.result(result) }
  }

  test("returns exceptions that result from submitting a task to the pool") {
    val executor = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue(1))
    val pool     = FuturePool(executor)

    val source   = new Promise[Int]
    val blocker1  = pool { Await.result(source) } // occupy the thread
    val blocker2  = pool { Await.result(source) } // fill the queue

    val rv = pool { "yay!" }

    assert(rv.isDefined === true)
    intercept[RejectedExecutionException] { Await.result(rv) }

    source.setValue(1)
  }

  test("interrupt threads when interruptible") {
    val executor = Executors.newFixedThreadPool(1).asInstanceOf[ThreadPoolExecutor]
    val started = new Promise[Unit]
    val interrupted = new Promise[Unit]
    val ipool = FuturePool.interruptible(executor)

    val f = ipool {
      try {
        started.setDone()
        Thread.sleep(Long.MaxValue)
      } catch { case exc: InterruptedException =>
        interrupted.setDone()
      }
    }

    Await.result(started)
    f.raise(new RuntimeException("foo"))
    intercept[RuntimeException] { Await.result(f) }
    assert(Await.result(interrupted) === ())
  }

  test("not interrupt threads when not interruptible") {
    val executor = Executors.newFixedThreadPool(1).asInstanceOf[ThreadPoolExecutor]
    val a = new Promise[Unit]
    val b = new Promise[Unit]
    val nipool = FuturePool(executor)

    val f = nipool {
      a.setDone()
      Await.result(b)
      1
    }

    Await.result(a)
    f.raise(new RuntimeException("foo"))
    b.setDone()
    assert(Await.result(f) === 1)
  }
}
