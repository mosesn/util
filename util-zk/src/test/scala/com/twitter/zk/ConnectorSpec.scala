package com.twitter.zk


import org.scalatest.{WordSpec, Matchers}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import com.twitter.util.Future

class ConnectorSpec extends WordSpec with Matchers with MockitoSugar {
  "Connector.RoundRobin" should  {
    "require underlying connections" in {
      intercept[Exception] {
        Connector.RoundRobin()
      }
    }

    "dispatch requests across underlying connectors" should {
      val nConnectors = 3
      val connectors = 1 to nConnectors map { _ => mock[Connector] }
      val connector = Connector.RoundRobin(connectors: _*)

      "apply" ignore {
        connectors foreach {
          _ apply() shouldBe Future.never
        }
        (1 to 2 * nConnectors) foreach { _ =>
          connector()
        }
        connectors foreach { c =>
          verify(c, times(2)).apply()
        }
      }

      "release" ignore {
        connectors foreach {
          _ release() shouldBe Future.never
        }
        (1 to 2) foreach { _ =>
          connector.release()
        }
        connectors foreach { c =>
          verify(c, times(2)).release()
        }
      }
    }
  }
}
