package com.twitter.zk

/**
 * @author ver@twitter.com
 */


import org.scalatest.{WordSpec, Matchers}
import org.scalatest.mock.MockitoSugar

class ZNodeSpec extends WordSpec with Matchers with MockitoSugar {
  "ZNode" should  {
    val zk = mock[ZkClient]
    def pathTest(path: String, parent: String, name: String) {
      val znode = ZNode(zk, path)
      path in {
        "parentPath" in { znode.parentPath shouldEqual parent }
        "name"       in { znode.name       shouldEqual name   }
      }
    }

    pathTest("/", "/", "")
    pathTest("/some/long/path/to/a/znode", "/some/long/path/to/a", "znode")
    pathTest("/path", "/", "path")

    "hash together" in {
      val zs = (0 to 1) map { _ => ZNode(zk, "/some/path") }
      val table = Map(zs(0) -> true)
      table should contain key (zs(0))
      table should contain key (zs(1))
    }
  }
}
