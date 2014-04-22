package com.twitter.util

import org.scalatest.{WordSpec, Matchers}
import util.Random



class U64Spec extends WordSpec with Matchers {
  import U64._

  "comparable" in {
    {
      val a = 0x0000000000000001L
      a shouldEqual(1)
      val b = 0x0000000000000002L
      b shouldEqual(2)

      a.u64_<(b) shouldBe true
      b.u64_<(a) shouldBe false
    }

    {
      val a = 0xFFFFFFFFFFFFFFFFL
      a shouldEqual(-1)
      val b = 0xFFFFFFFFFFFFFFFEL
      b shouldEqual(-2)

      a.u64_<(b) shouldBe false
      b.u64_<(a) shouldBe true
    }

    {
      val a = 0xFFFFFFFFFFFFFFFFL
      a shouldEqual(-1)
      val b = 0x0000000000000001L
      b shouldEqual(1)

      a.u64_<(b) shouldBe false
      b.u64_<(a) shouldBe true
    }
  }

  "comparable in range" in {
    0L.u64_within(0, 1)    shouldBe false
    0L.u64_contained(0, 1) shouldBe true

    // (inverted range)
    0L.u64_within(-1, 1) shouldBe false
    1L.u64_within(-1, 1) shouldBe false
    2L.u64_within(-1, 1) shouldBe false

    0xFFFFFFFFFFFFFFFEL.u64_within(0xFFFFFFFFFFFFFFFDL, 0xFFFFFFFFFFFFFFFFL) shouldBe true
    0xFFFFFFFFFFFFFFFDL.u64_within(0xFFFFFFFFFFFFFFFDL, 0xFFFFFFFFFFFFFFFFL) shouldBe false
    0xFFFFFFFFFFFFFFFFL.u64_within(0xFFFFFFFFFFFFFFFDL, 0xFFFFFFFFFFFFFFFFL) shouldBe false

    0xFFFFFFFFFFFFFFFEL.u64_contained(0xFFFFFFFFFFFFFFFDL, 0xFFFFFFFFFFFFFFFFL) shouldBe true
    0xFFFFFFFFFFFFFFFDL.u64_contained(0xFFFFFFFFFFFFFFFDL, 0xFFFFFFFFFFFFFFFFL) shouldBe true
    0xFFFFFFFFFFFFFFFFL.u64_contained(0xFFFFFFFFFFFFFFFDL, 0xFFFFFFFFFFFFFFFFL) shouldBe true

    // Bit flip area!
    0x7FFFFFFFFFFFFFFFL.u64_within(0x7FFFFFFFFFFFFFFFL, 0x8000000000000000L) shouldBe false
    0x8000000000000000L.u64_within(0x7FFFFFFFFFFFFFFFL, 0x8000000000000000L) shouldBe false

    0x7FFFFFFFFFFFFFFFL.u64_contained(0x7FFFFFFFFFFFFFFFL, 0x8000000000000000L) shouldBe true
    0x8000000000000000L.u64_contained(0x7FFFFFFFFFFFFFFFL, 0x8000000000000000L) shouldBe true

    0x7FFFFFFFFFFFFFFAL.u64_within(0x7FFFFFFFFFFFFFFAL, 0x800000000000000AL) shouldBe false
    0x7FFFFFFFFFFFFFFBL.u64_within(0x7FFFFFFFFFFFFFFAL, 0x800000000000000AL) shouldBe true
    0x7FFFFFFFFFFFFFFFL.u64_within(0x7FFFFFFFFFFFFFFAL, 0x800000000000000AL) shouldBe true
    0x8000000000000000L.u64_within(0x7FFFFFFFFFFFFFFAL, 0x800000000000000AL) shouldBe true
    0x8000000000000001L.u64_within(0x7FFFFFFFFFFFFFFAL, 0x800000000000000AL) shouldBe true
    0x8000000000000009L.u64_within(0x7FFFFFFFFFFFFFFAL, 0x800000000000000AL) shouldBe true
    0x800000000000000AL.u64_within(0x7FFFFFFFFFFFFFFAL, 0x800000000000000AL) shouldBe false
  }

  "divisible" in {
    10L.u64_/(5L) shouldEqual(2L)
    0xFFFFFFFFFFFFFFFFL / 0x0FFFFFFFFFFFFFFFL shouldEqual(0L)
    0xFFFFFFFFFFFFFFFFL.u64_/(0x0FFFFFFFFFFFFFFFL) shouldEqual(16L)

    0x7FFFFFFFFFFFFFFFL.u64_/(2) shouldEqual(0x3FFFFFFFFFFFFFFFL)

    0x8000000000000000L / 2 shouldEqual(0xc000000000000000L)
    0x8000000000000000L.u64_/(2) shouldEqual(0x4000000000000000L)

    0x8000000000000000L.u64_/(0x8000000000000000L) shouldEqual(1)
    0x8000000000000000L.u64_/(0x8000000000000001L) shouldEqual(0)

    0xFF00000000000000L.u64_/(0x0F00000000000000L) shouldEqual(0x11L)
    0x8F00000000000000L.u64_/(0x100) shouldEqual(0x008F000000000000L)
    0x8000000000000000L.u64_/(3) shouldEqual(0x2AAAAAAAAAAAAAAAL)
  }

  "ids" in {
    "survive conversions" in {
      val rng = new Random

      (0 until 10000).foreach { _ =>
        val id = rng.nextLong
        id should be (id.toU64ByteArray.toU64Long)
        id should be (id.toU64ByteArray.toU64HexString.toU64ByteArray.toU64Long)
      }
    }

    "be serializable" in {
      0L.toU64HexString shouldEqual("0000000000000000")
      0x0102030405060700L.toU64HexString shouldEqual("0102030405060700")
      0xFFF1F2F3F4F5F6F7L.toU64HexString shouldEqual("fff1f2f3f4f5f6f7")
    }

    "convert from short hex string" in {
      new RichU64String("7b").toU64Long shouldEqual 123L
    }

    "don't silently truncate" in {
      intercept[NumberFormatException] {
        new RichU64String("318528893302738945")
      }
    }
  }
}
