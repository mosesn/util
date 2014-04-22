package com.twitter.concurrent


import org.scalatest.{WordSpec, Matchers}
import com.twitter.util.Return

class TxSpec extends WordSpec with Matchers {
  "Tx.twoParty" should  {
    "commit when everything goes dandy" in {
      val (stx, rtx) = Tx.twoParty(123)
      val sf = stx.ack()
      sf.poll shouldEqual None
      val rf = rtx.ack()
      sf.poll shouldEqual Some(Return(Tx.Commit(())))
      rf.poll shouldEqual Some(Return(Tx.Commit(123)))
    }

    "abort when receiver nacks" in {
      val (stx, rtx) = Tx.twoParty(123)
      val sf = stx.ack()
      sf.poll shouldEqual None
      rtx.nack()
      sf.poll shouldEqual Some(Return(Tx.Abort))
    }

    "abort when sender nacks" in {
      val (stx, rtx) = Tx.twoParty(123)
      val rf = rtx.ack()
      rf.poll shouldEqual None
      stx.nack()
      rf.poll shouldEqual Some(Return(Tx.Abort))
    }

    "complain on ack ack" in {
      val (stx, rtx) = Tx.twoParty(123)
      rtx.ack()
      rtx.ack() should throwA(Tx.AlreadyAckd)
    }

    "complain on ack nack" in {
      val (stx, rtx) = Tx.twoParty(123)
      rtx.ack()
      rtx.nack() should throwA(Tx.AlreadyAckd)
    }

    "complain on nack ack" in {
      val (stx, rtx) = Tx.twoParty(123)
      rtx.nack()
      rtx.ack() should throwA(Tx.AlreadyNackd)
    }

    "complain on nack nack" in {
      val (stx, rtx) = Tx.twoParty(123)
      rtx.nack()
      rtx.nack() should throwA(Tx.AlreadyNackd)
    }

    "complain when already done" in {
      val (stx, rtx) = Tx.twoParty(123)
      stx.ack()
      rtx.ack()
      stx.ack() should throwA(Tx.AlreadyDone)
    }
  }
}
