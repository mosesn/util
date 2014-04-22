package com.twitter.zk


import org.scalatest.{WordSpec, Matchers}
import org.scalatest.mock.MockitoSugar
import com.twitter.util.Future

class ConnectorSpec extends WordSpec with Matchers with MockitoSugar {
  "Connector.RoundRobin" should  {
    "require underlying connections" in {
      intercept[Exception] {
        Connector.RoundRobin()
      }
    }

    "dispatch requests across underlying connectors" in {
      val nConnectors = 3
      val connectors = 1 to nConnectors map { _ => mock[Connector] }
      val connector = Connector.RoundRobin(connectors: _*)

      "apply" in {
        connectors foreach { x =>
          (x apply()) should equals (Future.never)
        }
        (1 to 2 * nConnectors) foreach { _ =>
          connector()
        }
        connectors foreach { c =>
          there were two(c).apply()
        }
      }

      "release" in {
        connectors foreach {
          _ release() returns Future.never
        }
        (1 to 2) foreach { _ =>
          connector.release()
        }
        connectors foreach { c =>
          there were two(c).release()
        }
      }
    }
  }
}
