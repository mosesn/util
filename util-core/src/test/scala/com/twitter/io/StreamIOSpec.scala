package com.twitter.io

import scala.util.Random


import org.scalatest.{WordSpec, Matchers}

class StreamIOSpec extends WordSpec with Matchers {
  "StreamIO.copy" should  {
    "copy the entire stream" in {
      val buf = new Array[Byte](2048)
      (new Random).nextBytes(buf)
      val bis = new java.io.ByteArrayInputStream(buf)
      val bos = new java.io.ByteArrayOutputStream()
      StreamIO.copy(bis, bos)
      bos.toByteArray.toSeq shouldEqual(buf.toSeq)
    }

    "produce empty streams from empty streams" in {
      val bis = new java.io.ByteArrayInputStream(new Array[Byte](0))
      val bos = new java.io.ByteArrayOutputStream()
      StreamIO.copy(bis, bos)
      bos.size shouldEqual(0)
    }
  }
}
