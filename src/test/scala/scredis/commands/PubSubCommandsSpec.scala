package scredis.commands

import org.scalatest._
import org.scalatest.concurrent._

import scredis._
import scredis.PubSubMessage._
import scredis.protocol.requests.PubSubRequests
import scredis.serialization.UTF8StringWriter
import scredis.exceptions._
import scredis.tags._
import scredis.util.TestUtils._

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import scala.concurrent.Promise

import java.util.concurrent.{ LinkedBlockingQueue, TimeUnit }

class PubSubCommandsSpec extends WordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with ScalaFutures {
  
  implicit class BlockingLinkedBlockingQueue[A](queue: LinkedBlockingQueue[A]) {
    def poll(count: Int): List[A] = {
      val buffer = ListBuffer[A]()
      for (i <- 1 to count) {
        val elem = queue.poll(1, TimeUnit.SECONDS)
        if (elem != null) {
          buffer += elem
        }
      }
      buffer.toList
    }
  }
  
  implicit def stringToByteArray(str: String): Array[Byte] = UTF8StringWriter.write(str)
  
  private val publisher = Client(port = 6380, passwordOpt = Some("foobar"))
  private val client = SubscriberClient(port = 6380, passwordOpt = Some("foobar"))
  private val client2 = SubscriberClient(port = 6380, passwordOpt = Some("foobar"))
  private val client3 = SubscriberClient(port = 6380, passwordOpt = Some("foobar"))
  private val client4 = SubscriberClient(port = 6380, passwordOpt = Some("foobar"))
  private val SomeValue = "HelloWorld!虫àéç蟲"

  private val subscribes = new LinkedBlockingQueue[Subscribe]()
  private val unsubscribes = new LinkedBlockingQueue[Unsubscribe]()
  private val messages = new LinkedBlockingQueue[Message]()
  
  private val pSubscribes = new LinkedBlockingQueue[PSubscribe]()
  private val pUnsubscribes = new LinkedBlockingQueue[PUnsubscribe]()
  private val pMessages = new LinkedBlockingQueue[PMessage]()
  
  private val pf: PartialFunction[PubSubMessage, Unit] = {
    case m: Subscribe => subscribes.put(m)
    case m: Message => messages.put(m)
    case m: Unsubscribe => unsubscribes.put(m)
    case m: PSubscribe => pSubscribes.put(m)
    case m: PMessage => pMessages.put(m)
    case m: PUnsubscribe => pUnsubscribes.put(m)
  }

  private def clear(): Unit = {
    subscribes.clear()
    unsubscribes.clear()
    messages.clear()
    pSubscribes.clear()
    pUnsubscribes.clear()
    pMessages.clear()
  }

  PubSubRequests.Publish.toString when {
    "there are no clients subscribed to the channel" should {
      "return 0" taggedAs (V200) in {
        publisher.publish("CHANNEL", "MESSAGE").futureValue should be (0)
      }
    }
    "some clients have subscribed to the channel" should {
      "return the number of subscribed clients that received the message" taggedAs (V200) in {
        val promise2 = Promise[Unit]()
        client2.subscribe("CHANNEL") {
          case Subscribe(_, _) => promise2.success(())
        }
        val promise3 = Promise[Unit]()
        client3.subscribe("CHANNEL") {
          case Subscribe(_, _) => promise3.success(())
        }
        promise2.future.futureValue should be (())
        promise3.future.futureValue should be (())
        publisher.publish("CHANNEL", SomeValue).futureValue should be (2)
        client2.unsubscribe().futureValue should be (0)
        client3.unsubscribe().futureValue should be (0)
      }
    }
  }

  PubSubRequests.Subscribe.toString when {
    "some messages get published" should {
      Given("that the messages get published to non-subscribed channels")
      "not receive any messages" taggedAs (V200) in {
        client.subscribe("CHANNEL1", "CHANNEL2")(pf).futureValue should be (2)
        publisher.publish("CHANNEL3", "A").futureValue should be (0)
        publisher.publish("CHANNEL3", "B").futureValue should be (0)
        publisher.publish("CHANNEL4", "C").futureValue should be (0)
        client.unsubscribe().futureValue should be (0)
        
        subscribes.poll(2) should contain theSameElementsAs List(
          Subscribe("CHANNEL1", 1),
          Subscribe("CHANNEL2", 2)
        )
        messages.poll(0) should be (empty)
        unsubscribes.poll(2) should have size (2)
        clear()
      }
      Given("that some messages get published to a subscribed channel")
      "receive the messages belonging to the subscribed channels" taggedAs (V200) in {
        client.subscribe("CHANNEL1", "CHANNEL2")(pf).futureValue should be (2)
        publisher.publish("CHANNEL1", "A").futureValue should be (1)
        publisher.publish("CHANNEL3", "B").futureValue should be (0)
        publisher.publish("CHANNEL2", "C").futureValue should be (1)
        publisher.publish("CHANNEL1", "D").futureValue should be (1)
        
        client.subscribe("CHANNEL3")(pf).futureValue should be (3)
        publisher.publish("CHANNEL3", "E").futureValue should be (1)

        client.unsubscribe().futureValue should be (0)

        subscribes.poll(3) should contain theSameElementsAs List(
          Subscribe("CHANNEL1", 1),
          Subscribe("CHANNEL2", 2),
          Subscribe("CHANNEL3", 3)
        )
        
        messages.poll(4) should contain theSameElementsAs List(
          Message("CHANNEL1", "A"),
          Message("CHANNEL2", "C"),
          Message("CHANNEL1", "D"),
          Message("CHANNEL3", "E")
        )
        
        unsubscribes.poll(3) should have size (3)
        clear()
      }
    }
  }

  PubSubRequests.PSubscribe.toString when {
    "some messages get published" should {
      Given("that the messages get published to non-matched channels")
      "not receive any messages" taggedAs (V200) in {
        client.pSubscribe("MyC*", "*5")(pf).futureValue should be (2)
        publisher.publish("CHANNEL1", "A").futureValue should be (0)
        publisher.publish("CHANNEL2", "B").futureValue should be (0)
        publisher.publish("CHANNEL3", "C").futureValue should be (0)
        
        client.pUnsubscribe().futureValue should be (0)
        
        pSubscribes.poll(2) should contain theSameElementsAs List(
          PSubscribe("MyC*", 1),
          PSubscribe("*5", 2)
        )
        pMessages.poll(0) should be (empty)
        pUnsubscribes.poll(2) should have size (2)
        clear()
      }
      Given("that some messages get published to matching patterns")
      "receive the messages matching the patterns" taggedAs (V200) in {
        client.pSubscribe("MyC*", "*5")(pf).futureValue should be (2)
        publisher.publish("MyChannel1", "A").futureValue should be (1)
        publisher.publish("CHANNEL3", "B").futureValue should be (0)
        publisher.publish("MyChannel2", "C").futureValue should be (1)
        publisher.publish("CHANNEL5", "D").futureValue should be (1)

        client.pSubscribe("*")(pf).futureValue should be (3)
        publisher.publish("CHANNEL3", "E").futureValue should be (1)

        client.pUnsubscribe().futureValue should be (0)
        
        pSubscribes.poll(3) should contain theSameElementsAs List(
          PSubscribe("MyC*", 1),
          PSubscribe("*5", 2),
          PSubscribe("*", 3)
        )
        pMessages.poll(4) should contain theSameElementsAs List(
          PMessage("MyC*", "MyChannel1", "A"),
          PMessage("MyC*", "MyChannel2", "C"),
          PMessage("*5", "CHANNEL5", "D"),
          PMessage("*", "CHANNEL3", "E")
        )
        
        pUnsubscribes.poll(3) should have size (3)
        clear()
      }
    }
  }

  PubSubRequests.Unsubscribe.toString when {
    "unsubscribing from non-subscribed channels" should {
      "do nothing and still receive published messages" taggedAs (V200) in {
        client.subscribe("CHANNEL1", "CHANNEL2", "CHANNEL3")(pf).futureValue should be (3)
        client.unsubscribe("CHANNEL0", "CHANNEL4").futureValue should be (3)
        publisher.publish("CHANNEL1", "HELLO").futureValue should be (1)
        publisher.publish("CHANNEL2", "HELLO").futureValue should be (1)
        publisher.publish("CHANNEL3", "HELLO").futureValue should be (1)
        
        subscribes.poll(3) should contain theSameElementsAs List(
          Subscribe("CHANNEL1", 1),
          Subscribe("CHANNEL2", 2),
          Subscribe("CHANNEL3", 3)
        )
        messages.poll(3) should contain theSameElementsAs List(
          Message("CHANNEL1", "HELLO"),
          Message("CHANNEL2", "HELLO"),
          Message("CHANNEL3", "HELLO")
        )
        
        unsubscribes.poll(2) should have size (2)
        unsubscribes.forall(_.channelsCount == 3) should be (true)
        clear()
      }
    }
    "unsubscribing from subscribed channels" should {
      "unsubscribe and published messages should no longer be received" taggedAs (V200) in {
        client.unsubscribe("CHANNEL1").futureValue should be (2)
        publisher.publish("CHANNEL1", "HELLO").futureValue should be (0)
        publisher.publish("CHANNEL2", "HELLO").futureValue should be (1)
        publisher.publish("CHANNEL3", "HELLO").futureValue should be (1)
        client.unsubscribe().futureValue should be (0)
        publisher.publish("CHANNEL1", "HELLO").futureValue should be (0)
        publisher.publish("CHANNEL2", "HELLO").futureValue should be (0)
        publisher.publish("CHANNEL3", "HELLO").futureValue should be (0)
        
        subscribes.poll(0) should be (empty)
        messages.poll(2) should contain theSameElementsAs List(
          Message("CHANNEL2", "HELLO"),
          Message("CHANNEL3", "HELLO")
        )
        
        unsubscribes.poll(3) should have size (3)
        clear()
      }
    }
  }

  PubSubRequests.PUnsubscribe.toString when {
    "unsubscribing from non-subscribed patterns" should {
      "do nothing and still receive published messages" taggedAs (V200) in {
        client.pSubscribe("*1", "*2", "*3")(pf).futureValue should be (3)
        client.pUnsubscribe("*0", "*4").futureValue should be (3)
        publisher.publish("CHANNEL1", "HELLO").futureValue should be (1)
        publisher.publish("CHANNEL2", "HELLO").futureValue should be (1)
        publisher.publish("CHANNEL3", "HELLO").futureValue should be (1)
        
        pSubscribes.poll(3) should contain theSameElementsAs List(
          PSubscribe("*1", 1),
          PSubscribe("*2", 2),
          PSubscribe("*3", 3)
        )
        pMessages.poll(3) should contain theSameElementsAs List(
          PMessage("*1", "CHANNEL1", "HELLO"),
          PMessage("*2", "CHANNEL2", "HELLO"),
          PMessage("*3", "CHANNEL3", "HELLO")
        )
        pUnsubscribes.poll(2) should have size (2)
        pUnsubscribes.forall(_.patternsCount == 3) should be (true)
        clear()
      }
    }
    "unsubscribing from subscribed patterns" should {
      "unsubscribe and published messages should no longer be received" taggedAs (V200) in {
        client.pUnsubscribe("*1").futureValue should be (2)
        publisher.publish("CHANNEL1", "HELLO").futureValue should be (0)
        publisher.publish("CHANNEL2", "HELLO").futureValue should be (1)
        publisher.publish("CHANNEL3", "HELLO").futureValue should be (1)
        client.pUnsubscribe().futureValue should be (0)
        publisher.publish("CHANNEL1", "HELLO").futureValue should be (0)
        publisher.publish("CHANNEL2", "HELLO").futureValue should be (0)
        publisher.publish("CHANNEL3", "HELLO").futureValue should be (0)
        
        pSubscribes.poll(0) should be (empty)
        pMessages.poll(3) should contain theSameElementsAs List(
          PMessage("*2", "CHANNEL2", "HELLO"),
          PMessage("*3", "CHANNEL3", "HELLO")
        )
        pUnsubscribes.poll(3) should have size (3)
        clear()
      }
    }
  }
  
  "Automatic re-subscribe" when {
    "a subscribed client gets disconnected and reconnects" should {
      "preserve its subscriptions after reconnection" taggedAs (V2812) in {
        client.unsubscribe().futureValue should be (0)
        client.pUnsubscribe().futureValue should be (0)
        
        client.subscribe("CHANNEL1", "CHANNEL2")(pf).futureValue should be (2)
        
        subscribes.poll(2) should contain theSameElementsAs List(
          Subscribe("CHANNEL1", 1),
          Subscribe("CHANNEL2", 2)
        )
        
        client.pSubscribe("CH*", "*EL", "ASD*")(pf).futureValue should be (5)
        
        pSubscribes.poll(3) should contain theSameElementsAs List(
          PSubscribe("CH*", 3),
          PSubscribe("*EL", 4),
          PSubscribe("ASD*", 5)
        )
        
        clear()
        
        publisher.clientKillWithFilters(
          typeOpt = Some(ClientType.PubSub)
        ).futureValue should be (1)
        
        subscribes.poll(2) should have size (2)
        pSubscribes.poll(3) should have size (3)
        
        client.subscribe("CHANNEL3")(pf).futureValue should be (6)
        client.pSubscribe("*")(pf).futureValue should be (7)
        
        publisher.publish("CHANNEL1", "LOL").futureValue should be (3)
        
        messages.poll(1) should contain theSameElementsAs List(
          Message("CHANNEL1", "LOL")
        )
        
        pMessages.poll(2) should contain theSameElementsAs List(
          PMessage("CH*", "CHANNEL1", "LOL"),
          PMessage("*", "CHANNEL1", "LOL")
        )
        
        client.unsubscribe().futureValue should be (4)
        client.pUnsubscribe().futureValue should be (0)
        
        clear()
      }
    }
  }
  
  PubSubRequests.PubSubChannels.toString should {
    "list all active channels" taggedAs (V280) in {
      client.subscribe("CHANNEL1", "CHANNEL2", "CHANNEL3")(pf).futureValue should be (3)
      client.pSubscribe("BLAH*")(pf).futureValue should be (4)
      subscribes.poll(3) should contain theSameElementsAs List(
        Subscribe("CHANNEL1", 1),
        Subscribe("CHANNEL2", 2),
        Subscribe("CHANNEL3", 3)
      )
      pSubscribes.poll(1) should contain theSameElementsAs List(
        PSubscribe("BLAH*", 4)
      )
      publisher.pubSubChannels().futureValue should contain theSameElementsAs List(
        "CHANNEL1", "CHANNEL2", "CHANNEL3"
      )
      publisher.pubSubChannels(Some("CHAN*")).futureValue should contain theSameElementsAs List(
        "CHANNEL1", "CHANNEL2", "CHANNEL3"
      )
      publisher.pubSubChannels(Some("*")).futureValue should contain theSameElementsAs List(
        "CHANNEL1", "CHANNEL2", "CHANNEL3"
      )
      publisher.pubSubChannels(Some("*2")).futureValue should contain theSameElementsAs List(
        "CHANNEL2"
      )
      publisher.pubSubChannels(Some("BLAH*")).futureValue should be (empty)
      
      client.unsubscribe().futureValue should be (1)
      publisher.pubSubChannels().futureValue should be (empty)
      client.pUnsubscribe().futureValue should be (0)
      
      clear()
    }
  }
  
  PubSubRequests.PubSubNumSub.toString should {
    "return the number of subscribers for the specified channel(s)" taggedAs (V280) in {
      client.subscribe("CHANNEL1", "CHANNEL2", "CHANNEL3")(pf).futureValue should be (3)
      client.pSubscribe("BLAH*")(pf).futureValue should be (4)
      client2.subscribe("CHANNEL1", "CHANNEL2")(pf).futureValue should be (2)
      client3.subscribe("CHANNEL1")(pf).futureValue should be (1)
      subscribes.poll(6) should contain theSameElementsAs List(
        Subscribe("CHANNEL1", 1),
        Subscribe("CHANNEL2", 2),
        Subscribe("CHANNEL3", 3),
        Subscribe("CHANNEL1", 1),
        Subscribe("CHANNEL2", 2),
        Subscribe("CHANNEL1", 1)
      )
      pSubscribes.poll(1) should contain theSameElementsAs List(
        PSubscribe("BLAH*", 4)
      )
      publisher.pubSubNumSub().futureValue should be (empty)
      publisher.pubSubNumSub(
        "CHANNEL1", "CHANNEL2", "CHANNEL3", "CHANNEL4"
      ).! should contain theSameElementsAs List(
        ("CHANNEL1", 3), ("CHANNEL2", 2), ("CHANNEL3", 1), ("CHANNEL4", 0)
      )
      
      client.unsubscribe().futureValue should be (1)
      client.pUnsubscribe().futureValue should be (0)
      publisher.pubSubNumSub(
        "CHANNEL1", "CHANNEL2", "CHANNEL3", "CHANNEL4"
      ).! should contain theSameElementsAs List(
        ("CHANNEL1", 2), ("CHANNEL2", 1), ("CHANNEL3", 0), ("CHANNEL4", 0)
      )
      
      client2.unsubscribe().futureValue should be (0)
      publisher.pubSubNumSub(
        "CHANNEL1", "CHANNEL2", "CHANNEL3", "CHANNEL4"
      ).! should contain theSameElementsAs List(
        ("CHANNEL1", 1), ("CHANNEL2", 0), ("CHANNEL3", 0), ("CHANNEL4", 0)
      )
      
      client3.unsubscribe().futureValue should be (0)
      publisher.pubSubNumSub(
        "CHANNEL1", "CHANNEL2", "CHANNEL3", "CHANNEL4"
      ).! should contain theSameElementsAs List(
        ("CHANNEL1", 0), ("CHANNEL2", 0), ("CHANNEL3", 0), ("CHANNEL4", 0)
      )
      
      clear()
    }
  }
  
  PubSubRequests.PubSubNumPat.toString should {
    "return the total number of pattern(s) all the clients are subscribed to" taggedAs (V280) in {
      client.subscribe("CHANNEL1")(pf).futureValue should be (1)
      client.pSubscribe("*1", "*2", "*3")(pf).futureValue should be (4)
      client2.pSubscribe("*1", "*2")(pf).futureValue should be (2)
      client3.pSubscribe("*1")(pf).futureValue should be (1)
      subscribes.poll(1) should contain theSameElementsAs List(
        Subscribe("CHANNEL1", 1)
      )
      pSubscribes.poll(6) should contain theSameElementsAs List(
        PSubscribe("*1", 2),
        PSubscribe("*2", 3),
        PSubscribe("*3", 4),
        PSubscribe("*1", 1),
        PSubscribe("*2", 2),
        PSubscribe("*1", 1)
      )
      publisher.pubSubNumPat().futureValue should be (6)
      
      client.unsubscribe().futureValue should be (3)
      client.pUnsubscribe().futureValue should be (0)
      publisher.pubSubNumPat().futureValue should be (3)
      
      client2.pUnsubscribe().futureValue should be (0)
      publisher.pubSubNumPat().futureValue should be (1)
      
      client3.pUnsubscribe().futureValue should be (0)
      publisher.pubSubNumPat().futureValue should be (0)
      
      clear()
    }
  }

  override def afterAll() {
    publisher.quit().!
    client.quit().!
    client2.quit().!
    client3.quit().!
    client4.quit().!
  }
  
}