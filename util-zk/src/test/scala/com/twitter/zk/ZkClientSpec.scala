package com.twitter.zk 

import com.twitter.conversions.time._
import com.twitter.logging.{Level, Logger}
import com.twitter.util._
import org.apache.zookeeper._
import org.apache.zookeeper.data.{ACL, Stat}
import org.scalatest.{WordSpec, Matchers}
import org.scalatest.mock.MockitoSugar
import org.mockito._
import org.mockito.Mockito._
import org.mockito.Matchers.{eq => meq, _}

import scala.collection.JavaConverters._
import scala.collection.Set

class ZkClientSpec extends WordSpec with Matchers with MockitoSugar {
  Logger.get("").setLevel(Level.FATAL)

  val zk = mock[ZooKeeper]
  class TestZkClient extends ZkClient {
    val connector = new Connector {
      def apply() = Future(zk)
      def release() = Future.Done
    }
  }
  def zkClient = new TestZkClient

  implicit val javaTimer = new JavaTimer(true)

  /*
   * ZooKeeper expectation wrappers
   */

  def create(path: String,
             data: Array[Byte] = "".getBytes,
             acls: Seq[ACL] = zkClient.acl,
             mode: CreateMode = zkClient.mode)
            (wait: => Future[String]) {
    val cb = ArgumentCaptor.forClass(classOf[AsyncCallback.StringCallback])
    when(zk.create(meq(path),
        meq(data), meq(acls.asJava),
        meq(mode), cb.capture(),
        meq(null))) thenReturn {
      val cbValue = cb.getValue
      wait onSuccess { newPath =>
        cbValue.processResult(0, path, null, newPath)
      } onFailure { case ke: KeeperException =>
        cbValue.processResult(ke.code.intValue, path, null, null)
      }
      ()
    }
  }

  def delete(path: String, version: Int)(wait: => Future[Unit]) {
    val cb = ArgumentCaptor.forClass(classOf[AsyncCallback.VoidCallback])
    when(zk.delete(meq(path), meq(version), cb.capture(), meq(null))) thenReturn {
      val cbValue = cb.getValue
      wait onSuccess { _ =>
        cbValue.processResult(0, path, null)
      } onFailure { case ke: KeeperException =>
        cbValue.processResult(ke.code.intValue, path, null)
      }
      ()
    }
  }

  def exists(path: String)(stat: => Future[Stat]) {
    val cb = ArgumentCaptor.forClass(classOf[AsyncCallback.StatCallback])
    when(zk.exists(meq(path), meq(false), cb.capture(), meq(null))) thenReturn {
      val cbValue = cb.getValue
      stat onSuccess {
        cbValue.processResult(0, path, null, _)
      } onFailure { case ke: KeeperException =>
        cbValue.processResult(ke.code.intValue, path, null, null)
      }
      ()
    }
  }

  def watch(path: String)(stat: => Future[Stat])(update: => Future[WatchedEvent]) {
    val watcher = ArgumentCaptor.forClass(classOf[Watcher])
    val cb = ArgumentCaptor.forClass(classOf[AsyncCallback.StatCallback])
    when(zk.exists(meq(path), watcher.capture(), cb.capture(),
        meq(null))) thenReturn {
      val cbValue = cb.getValue
      stat onSuccess {
        cbValue.processResult(0, path, null, _)
      } onFailure { case ke: KeeperException =>
        cbValue.processResult(ke.code.intValue, path, null, null)
      }
      update onSuccess { watcher.getValue.process(_) }
      ()
    }
  }

  def getChildren(path: String)(children: => Future[ZNode.Children]) {
    val cb = ArgumentCaptor.forClass(classOf[AsyncCallback.Children2Callback])
    when(zk.getChildren(meq(path),
        meq(false), cb.capture(),
        meq(null))) thenReturn {
      val cbValue = cb.getValue
      children onSuccess { znode =>
        cbValue.processResult(0, path, null, znode.children.map { _.name }.toList.asJava, znode.stat)
      } onFailure { case ke: KeeperException =>
        cbValue.processResult(ke.code.intValue, path, null, null, null)
      }
      ()
    }
  }

  def watchChildren(path: String)
                   (children: => Future[ZNode.Children])
                   (update: => Future[WatchedEvent]) {
    val w = ArgumentCaptor.forClass(classOf[Watcher])
    val cb = ArgumentCaptor.forClass(classOf[AsyncCallback.Children2Callback])
    when(zk.getChildren(meq(path), w.capture(), cb.capture(),
        meq(null))) thenReturn {
      val cbValue = cb.getValue
      children onSuccess { case ZNode.Children(znode, stat, children) =>
        cbValue.processResult(0, path, null, children.map { _.name }.toList.asJava, stat)
      } onFailure { case ke: KeeperException =>
        cbValue.processResult(ke.code.intValue, path, null, null, null)
      }
      update onSuccess { w.getValue.process(_) }
      ()
    }
  }

  def getData(path: String)(result: => Future[ZNode.Data]) {
    val cb = ArgumentCaptor.forClass(classOf[AsyncCallback.DataCallback])
    when(zk.getData(meq(path), meq(false), cb.capture(), meq(null))) thenReturn {
      val cbValue = cb.getValue
      result onSuccess { z =>
        cbValue.processResult(0, path, null, z.bytes, z.stat)
      } onFailure { case ke: KeeperException =>
        cbValue.processResult(ke.code.intValue, path, null, null, null)
      }
      ()
    }
  }
  def watchData(path: String)(result: => Future[ZNode.Data])(update: => Future[WatchedEvent]) {
    val w = ArgumentCaptor.forClass(classOf[Watcher])
    val cb = ArgumentCaptor.forClass(classOf[AsyncCallback.DataCallback])
    when(zk.getData(meq(path),
        w.capture(), cb.capture(),
        meq(null))) thenReturn {
      val cbValue = cb.getValue
      result onSuccess { z =>
        cbValue.processResult(0, path, null, z.bytes, z.stat)
      } onFailure { case ke: KeeperException =>
        cbValue.processResult(ke.code.intValue, path, null, null, null)
      }
      update onSuccess { w.getValue.process(_) }
      ()
    }
  }

  def setData(path: String, data: Array[Byte], version: Int)
             (wait: => Future[Stat]) {
    val cb = ArgumentCaptor.forClass(classOf[AsyncCallback.StatCallback])
    when(zk.setData(meq(path), meq(data), meq(version),
        cb.capture(), meq(null))) thenReturn {
      val cbValue = cb.getValue
      wait onSuccess { stat =>
        cbValue.processResult(0, path, null, stat)
      } onFailure { case ke: KeeperException =>
        cbValue.processResult(ke.code.intValue, path, null, null)
      }
      ()
    }
  }

  def sync(path: String)
          (wait: Future[Unit]) {
    val cb = ArgumentCaptor.forClass(classOf[AsyncCallback.VoidCallback])
    when(zk.sync(meq(path), cb.capture(), meq(null))) thenReturn {
      val cbValue = cb.getValue
      wait onSuccess { _ =>
        cbValue.processResult(0, path, null)
      } onFailure { case ke: KeeperException =>
        cbValue.processResult(ke.code.intValue, path, null)
      }
      ()
    }
  }

  "ZkClient" should  {
    "apply" in {
      val path = "/root/path/to/a/node"
      val znode = zkClient(path)
      znode shouldBe a[ZNode]
      znode.path shouldEqual path
    }

    "retry" should {
      val connectionLoss = new KeeperException.ConnectionLossException

      "retry KeeperException.ConnectionLossException until completion" in {
        var i = 0
        Await.ready(zkClient.withRetries(3).retrying { _ =>
          i += 1
          Future.exception(connectionLoss)
        }.onSuccess { _ =>
          fail("Unexpected success")
        }.handle { case e: KeeperException.ConnectionLossException =>
          e shouldBe connectionLoss
          i shouldEqual 4
        })
      }

      "not retry on success" in {
        var i = 0
        Await.ready(zkClient.withRetries(3).retrying { _ =>
          i += 1
          Future.Done
        }.onSuccess { _ =>
          i shouldEqual 1
        })
      }

      "convert exceptions to Futures" in {
        val rex = new RuntimeException
        var i = 0
        Await.ready(zkClient.withRetries(3).retrying { _ =>
          i += 1
          throw rex
        }.onSuccess { _ =>
          fail("Unexpected success")
        }.handle { case e: RuntimeException =>
          e shouldBe rex
          i shouldEqual 1
        })
      }

      "only retry when instructed to" in {
        var i = 0
        Await.ready(zkClient.retrying { _ =>
          i += 1
          Future.exception(connectionLoss)
        }.onSuccess { _ =>
          fail("Shouldn't have succeeded")
        }.handle { case e: KeeperException.ConnectionLossException =>
          i shouldEqual 1
        })
      }
    }

    "transform" should {
      val acl = ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala
      val mode = CreateMode.EPHEMERAL_SEQUENTIAL
      val retries = 37
      val retryPolicy = new RetryPolicy.Basic(retries)

      "withAcl" in {
        val transformed = zkClient.withAcl(acl)
        transformed shouldBe a[ZkClient]
        transformed.acl shouldEqual acl
      }

      "withMode" in {
        val transformed = zkClient.withMode(mode)
        transformed shouldBe a[ZkClient]
        transformed.mode shouldEqual mode
      }

      "withRetries" in {
        val transformed = zkClient.withRetries(retries)
        transformed shouldBe a[ZkClient]
        assert(transformed.retryPolicy match {
          case RetryPolicy.Basic(r) => r == retries
          case _                    => false
        })
      }

      "withRetryPolicy" in {
        val transformed = zkClient.withRetryPolicy(retryPolicy)
        transformed shouldBe a[ZkClient]
        transformed.retryPolicy shouldBe retryPolicy
      }

      "chained" in {
        val transformed = zkClient.withAcl(acl).withMode(mode).withRetryPolicy(retryPolicy)
        transformed shouldBe a[ZkClient]
        transformed.acl shouldEqual acl
        transformed.mode shouldEqual mode
        transformed.retryPolicy shouldBe retryPolicy
      }
    }
  }

  "ZNode" should  {
    "apply a relative path to a ZNode" in {
      val znode = zkClient("/some/path")
      znode("to/child").path shouldEqual "/some/path/to/child"
    }

    "create" should {
      val path = "/root/path/to/a/node"

      "with data" in {
        val data = "blah".getBytes
        create(path, data)(Future(path))
        assert(Await.result(zkClient(path).create(data)) match {
          case ZNode(p) => p == path
        })
      }

      "error" in {
        val data = null
        create(path, data)(Future.exception(new KeeperException.NodeExistsException(path)))
        Await.ready(zkClient(path).create(data) map { _ =>
          fail("Unexpected success")
        } handle { case e: KeeperException.NodeExistsException =>
          e.getPath shouldEqual path
        })
      }

      "sequential" in {
        val data = null

        create(path, data, mode = CreateMode.EPHEMERAL_SEQUENTIAL)(Future(path + "0000"))

        val newPath = Await.result(
          zkClient(path).create(data, mode = CreateMode.EPHEMERAL_SEQUENTIAL))
        newPath.name shouldEqual "node0000"
      }

      "a child" in {
        val childPath = path + "/child"

        "with a name and data" in {
          val data = "blah".getBytes
          create(childPath, data)(Future(childPath))
          val znode = Await.result(zkClient(path).create(data, child = Some("child")))
          znode.path shouldEqual childPath
        }

        "error" in {
          val data = null
          create(childPath, data) {
            Future.exception(new KeeperException.NodeExistsException(childPath))
          }
          Await.ready(zkClient(path).create(data, child = Some("child")) map { _ =>
            fail("Unexpected success")
          } handle { case e: KeeperException.NodeExistsException =>
            e.getPath shouldEqual childPath
          })
        }

        "sequential, with an empty name" in {
          val data = null

          create(path+"/", data, mode = CreateMode.EPHEMERAL_SEQUENTIAL)(Future(path+"/0000"))

          Await.result(zkClient(path).create(data,
            child = Some(""),
            mode = CreateMode.EPHEMERAL_SEQUENTIAL
          )).path shouldEqual path + "/0000"
        }
      }
    }

    "delete" should {
      val version = 0
      val path = "/path"
      "ok" in {
        delete(path, version)(Future.Done)
        assert(Await.result(zkClient(path).delete(version)) match { 
          case ZNode(p) => p == path
        })
      }

      "error" in {
        delete(path, version)(Future.exception(new KeeperException.NoNodeException(path)))
        Await.ready(zkClient(path).delete(version) map { _ =>
          fail("Unexpected success")
        } handle { case e: KeeperException.NoNodeException =>
          e.getPath shouldEqual path
        })
      }
    }

    "exist" should {
      val znode = zkClient("/maybe/exists")
      val result = ZNode.Exists(znode, new Stat)

      "apply" should {
        "ok" in {
          exists(znode.path)(Future(result.stat))
          Await.result(znode.exists()) shouldEqual result
        }

        "error" in {
          exists(znode.path)(Future.exception(new KeeperException.NoNodeException(znode.path)))
          Await.ready(znode.exists() map { _ =>
            fail("Unexpected success")
          } handle { case e: KeeperException.NoNodeException =>
            e.getPath shouldEqual znode.path
          })
        }
      }

      "watch" in {
        val event = NodeEvent.Deleted(znode.path)
        watch(znode.path)(Future(result.stat))(Future(event))
        Await.ready(znode.exists.watch() onSuccess { case ZNode.Watch(r, update) =>
          r onSuccess { exists =>
            exists shouldEqual result
            update onSuccess {
              case e @ NodeEvent.Deleted(name) => e shouldEqual event
              case e => fail("Incorrect event: %s".format(e))
            }
          }
        })
      }

      "monitor" in {
        val deleted = NodeEvent.Deleted(znode.path)
        def expectZNodes(n: Int) {
          val results = 0 until n map { _ => ZNode.Exists(znode, new Stat) }
          results foreach { r =>
            watch(znode.path)(Future(r.stat))(Future(deleted))
          }
          val update = znode.exists.monitor()
          results foreach { result =>
            update syncWait() get() shouldEqual result
          }
        }

        "that updates several times" in {
          expectZNodes(3)
        }

        "handles session events properly" in {
          "AuthFailed" in {
            // this case is somewhat unrealistic
            watch(znode.path)(Future(new Stat))(Future(StateEvent.AuthFailed()))
            // does not reset
            val offer = znode.exists.monitor()
            offer syncWait() get() shouldEqual result
          }

          "Disconnected" in {
            val update = new Promise[WatchedEvent]
            watch(znode.path)(Future(new Stat))(Future(StateEvent.Disconnected()))
            watch(znode.path)(Future(new Stat))(Future(NodeEvent.Deleted(znode.path)))
            watch(znode.path)(Future.exception(new KeeperException.NoNodeException(znode.path)))(update)
            val offer = znode.exists.monitor()
            offer syncWait() get() shouldEqual result
            offer syncWait() get() shouldEqual result
            intercept[KeeperException.NoNodeException] {
              offer syncWait() get()
            }
            offer.sync().isDefined shouldBe false
          }

          "SessionExpired" in {
            watch(znode.path)(Future(new Stat))(Future(StateEvent.Disconnected()))
            watch(znode.path) {
              Future.exception(new KeeperException.SessionExpiredException)
            } {
              Future(StateEvent.Expired())
            }
            val offer = znode.exists.monitor()
            offer syncWait() get() shouldEqual result
            offer.sync().isDefined shouldBe false
          }
        }

        //"that updates many times" in {
        //  expectZNodes(20000)
        //}
      }
    }

    "getChildren" should {
      val znode = zkClient("/parent")
      val result = ZNode.Children(znode, new Stat, "doe" :: "ray" :: "me" :: Nil)

      "apply" should {
        "ok" in {
          getChildren(znode.path)(Future(result))
          Await.result(znode.getChildren()) shouldEqual result
        }

        "error" in {
          getChildren(znode.path) {
            Future.exception(new KeeperException.NoChildrenForEphemeralsException(znode.path))
          }
          Await.ready(znode.getChildren() map { _ =>
            fail("Unexpected success")
          } handle { case e: KeeperException.NoChildrenForEphemeralsException =>
            e.getPath shouldEqual znode.path
          })
        }
      }

      "watch" in {
        watchChildren(znode.path)(Future(result))(Future(NodeEvent.ChildrenChanged(znode.path)))
        Await.ready(znode.getChildren.watch().onSuccess { case ZNode.Watch(r, f) =>
          r onSuccess { case ZNode.Children(p, s, c) =>
            p shouldBe result.path
            s shouldBe result.stat
            f onSuccess {
              case NodeEvent.ChildrenChanged(name) => name shouldEqual znode.path
              case e => fail("Incorrect event: %s".format(e))
            }
          }
        })
      }

      "monitor" in {
        val znode = zkClient("/characters")
        val results = List(
            Seq("Angel", "Buffy", "Giles", "Willow", "Xander"),
            Seq("Angel", "Buffy", "Giles", "Spike", "Willow", "Xander"),
            Seq("Buffy", "Giles", "Willow", "Xander"),
            Seq("Angel", "Spike")) map { ZNode.Children(znode, new Stat, _) }
        results foreach { r =>
          watchChildren(znode.path)(Future(r))(Future(NodeEvent.ChildrenChanged(znode.path)))
        }

        val update = znode.getChildren.monitor()
        results foreach { result =>
          val r = update syncWait() get()
          r shouldEqual result
        }
      }
    }

    "getData" should {
      val znode = zkClient("/giles")
      val result = ZNode.Data(znode, new Stat, "good show, indeed".getBytes)

      "apply" should {
        "ok" in {
          getData(znode.path)(Future(result))
          Await.result(znode.getData()) shouldEqual result
        }

        "error" in {
          getData(znode.path) {
            Future.exception(new KeeperException.SessionExpiredException)
          }
          Await.ready(znode.getData() map { _ =>
            fail("Unexpected success")
          } handle { case e: KeeperException.SessionExpiredException =>
            e.getPath shouldEqual znode.path
          })
        }
      }

      "watch" in {
        watchData(znode.path) {
          Future(result)
        } {
          Future(NodeEvent.DataChanged(znode.path))
        }
        try {
          Await.ready(znode.getData.watch().onSuccess {
            case ZNode.Watch(Return(z), u) => {
              z shouldEqual result
              u onSuccess {
                case NodeEvent.DataChanged(name) => name shouldEqual znode.path
                case e => fail("Incorrect event: %s".format(e))
              }
            }
            case _ => fail("unexpected return value")
          })
        } catch { case e: Throwable => fail("unexpected error: %s".format(e)) }
      }

      "monitor" in {
        val results = List(
            "In every generation there is a chosen one.",
            "She alone will stand against the vampires the demons and the forces of darkness.",
            "She is the slayer.") map { text =>
          ZNode.Data(znode, new Stat, text.getBytes)
        }
        results foreach { result =>
          watchData(znode.path)(Future(result)) {
            Future(NodeEvent.ChildrenChanged(znode.path))
          }
        }
        watchData(znode.path) {
          Future.exception(new KeeperException.SessionExpiredException)
        } {
          new Promise[WatchedEvent]
        }
        val update = znode.getData.monitor()
        try {
          results foreach { data =>
            update.syncWait().get() shouldEqual data
          }
        } catch { case e: Throwable =>
          fail("unexpected error: %s".format(e))
        }
      }
    }

    "monitorTree" should {
      // Lay out a tree of ZNode.Children
      val treeRoot = ZNode.Children(zkClient("/arboreal"), new Stat, 'a' to 'e' map { _.toString })

      val treeChildren = treeRoot +: ('a' to 'e').map { c =>
        ZNode.Children(treeRoot(c.toString), new Stat, 'a' to c map { _.toString })
      }.flatMap { z =>
        z +: z.children.map { c => ZNode.Children(c, new Stat, Nil) }
      }

      // Lay out node updates for the tree: Add a 'z' node to all nodes named 'a'
      val updateTree = treeChildren.collect {
        case z @ ZNode.Children(p, s, c) if (p.endsWith("/c")) => {
          val newChild = ZNode.Children(z("z"), new Stat, Nil)
          val kids = c.filterNot { _.path.endsWith("/") } :+ newChild
          ZNode.Children(ZNode.Exists(z, s), kids) :: newChild :: Nil
        }
      }.flatten

      // Initially, we should  get a ZNode.TreeUpdate for each node in the tree with only added nodes
      val expectedByPath = treeChildren.map { z =>
        z.path -> ZNode.TreeUpdate(z, z.children.toSet)
      }.toMap
      val updatesByPath = updateTree.map { z =>
        val prior: Set[ZNode] = expectedByPath.get(z.path).map { _.added }.getOrElse(Set.empty)
        z.path -> ZNode.TreeUpdate(z, z.children.toSet -- prior, prior -- z.children.toSet)
      }.toMap

      def okUpdates(event: String => WatchedEvent) {
        // Create promises for each node in the tree -- satisfying a promise will fire a
        // ChildrenChanged event for its associated node.
        val updatePromises = treeChildren.map { _.path -> new Promise[WatchedEvent] }.toMap
        treeChildren foreach { z =>
          watchChildren(z.path)(Future(z))(updatePromises(z.path))
        }
        updateTree foreach { z =>
          watchChildren(z.path)(Future(z))(new Promise[WatchedEvent])  // nohollaback
        }

        val offer = treeRoot.monitorTree()
        treeChildren foreach { _ =>
          val ztu = Await.result(offer sync(), 1.second)
          val e = expectedByPath(ztu.parent.path)
          ztu.parent shouldEqual e.parent
          ztu.added.map { _.path } shouldEqual e.added.map { _.path }.toSet
          ztu.removed shouldBe empty
        }
        updateTree foreach { z =>
          updatePromises get(z.path) foreach { _.setValue(event(z.path)) }
          val ztu = Await.result(offer sync(), 1.second)
          val e = updatesByPath(z.path)
          ztu.parent shouldEqual e.parent
          ztu.added.map { _.path } shouldEqual e.added.map { _.path }.toSet
          ztu.removed shouldBe empty
        }
        intercept[TimeoutException] {
          Await.result(offer.sync(), 1.second)
        }
      }

      "ok" in okUpdates { NodeEvent.ChildrenChanged(_) }
      "be resilient to disconnect" in okUpdates { _ => StateEvent.Disconnected() }

      "stop on session expiration" in {
        treeChildren foreach { z =>
          watchChildren(z.path)(Future(z))(Future(StateEvent.Disconnected()))
          watchChildren(z.path)(Future.exception(new KeeperException.SessionExpiredException)) {
            Future(StateEvent.Expired())
          }
        }

        val offer = treeRoot.monitorTree()
        treeChildren foreach { _ =>
          val ztu = Await.result(offer sync(), 1.second)
          val e = expectedByPath(ztu.parent.path)
          ztu.parent shouldEqual e.parent
          ztu.added.map { _.path } shouldEqual e.added.map { _.path }.toSet
          ztu.removed shouldBe empty
        }
        intercept[TimeoutException] {
          Await.result(offer.sync(), 1.second)
        }
      }
    }

    "set data" in {
      val znode = zkClient("/empty/node")
      val result = ZNode.Exists(znode, new Stat)
      val data = "word to your mother.".getBytes
      val version = 13
      "ok" in {
        setData(znode.path, data, version)(Future(result.stat))
        Await.result(znode.setData(data, version), 1.second) shouldEqual znode
      }

      "error" in {
        setData(znode.path, data, version) {
          Future.exception(new KeeperException.SessionExpiredException)
        }
        intercept[KeeperException.SessionExpiredException] {
          Await.result(znode.setData(data, version))
        }
      }
    }

    "sync" in {
      val znode = zkClient("/sync")

      "ok" in {
        sync(znode.path)(Future.Done)
        Await.result(znode.sync(), 1.second) shouldEqual znode
      }

      "error" in {
        sync(znode.path) {
          Future.exception(new KeeperException.SystemErrorException)
        }
        Await.ready(znode.sync() map { _ =>
          fail("Unexpected success")
        } handle { case e: KeeperException.SystemErrorException =>
          e.getPath shouldEqual znode.path
        }, 1.second)
      }
    }
  }

  Option { System.getProperty("com.twitter.zk.TEST_CONNECT") } foreach { connectString =>
    "A live server @ %s".format(connectString) should  {
      val zkClient = ZkClient(connectString, 1.second, 1.minute)
      val znode = zkClient("/")
      def after = { zkClient.release() }
      "have 'zookeeper' in '/'" in {
        Await.result(znode.getChildren(), 2.seconds).children.map { _.name }.contains("zookeeper")
      }
    }
  }
}
