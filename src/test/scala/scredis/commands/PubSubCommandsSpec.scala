package scredis.commands

import org.scalatest._
import org.scalatest.concurrent._

import scredis._
import scredis.PubSubMessage._
import scredis.protocol.requests.PubSubRequests
import scredis.exceptions._
import scredis.tags._
import scredis.util.TestUtils._

import scala.collection.mutable.ListBuffer
import scala.concurrent.Promise

class PubSubCommandsSpec extends WordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with Inside
  with ScalaFutures {
  
  private val publisher = Client()
  private val client = SubscriberClient()
  private val client2 = SubscriberClient()
  private val client3 = SubscriberClient()
  private val client4 = SubscriberClient()
  private val SomeValue = "HelloWorld!虫àéç蟲"

  private val subscribes = ListBuffer[Subscribe]()
  private val unsubscribes = ListBuffer[Unsubscribe]()
  private val messages = ListBuffer[Message]()
  
  private val pSubscribes = ListBuffer[PSubscribe]()
  private val pUnsubscribes = ListBuffer[PUnsubscribe]()
  private val pMessages = ListBuffer[PMessage]()
  
  private val pf: PartialFunction[PubSubMessage, Unit] = {
    case m: Subscribe => subscribes += m
    case m: Message => messages += m
    case m: Unsubscribe => unsubscribes += m
    case m: PSubscribe => pSubscribes += m
    case m: PMessage => pMessages += m
    case m: PUnsubscribe => pUnsubscribes += m
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
        client2.unsubscribe()
        client3.unsubscribe()
      }
    }
  }

  PubSubRequests.Subscribe.toString when {
    "some messages get published" should {
      Given("that the messages get published to non-subscribed channels")
      "not receive any messages" taggedAs (V200) in {
        client.subscribe("CHANNEL1", "CHANNEL2")(pf)
        Thread.sleep(200)
        publisher.publish("CHANNEL3", "A").futureValue should be (0)
        publisher.publish("CHANNEL3", "B").futureValue should be (0)
        publisher.publish("CHANNEL4", "C").futureValue should be (0)
        Thread.sleep(200)
        client.unsubscribe()
        Thread.sleep(200)
        subscribes.toList should contain theSameElementsAs List(
          Subscribe("CHANNEL1", 1),
          Subscribe("CHANNEL2", 2)
        )
        messages should be (empty)
        unsubscribes should have size (2)
        clear()
      }
      Given("that some messages get published to a subscribed channel")
      "receive the messages belonging to the subscribed channels" taggedAs (V200) in {
        client.subscribe("CHANNEL1", "CHANNEL2")(pf)
        Thread.sleep(200)
        publisher.publish("CHANNEL1", "A").futureValue should be (1)
        publisher.publish("CHANNEL3", "B").futureValue should be (0)
        publisher.publish("CHANNEL2", "C").futureValue should be (1)
        publisher.publish("CHANNEL1", "D").futureValue should be (1)
        
        client.subscribe("CHANNEL3")(pf)
        Thread.sleep(200)
        publisher.publish("CHANNEL3", "E").futureValue should be (1)

        Thread.sleep(200)
        client.unsubscribe()
        Thread.sleep(200)

        subscribes.toList should contain theSameElementsInOrderAs List(
          Subscribe("CHANNEL1", 1),
          Subscribe("CHANNEL2", 2),
          Subscribe("CHANNEL3", 3)
        )
        
        inside(messages.toList) {
          case List(message1, message2, message3, message4) => {
            message1.channel should be ("CHANNEL1")
            message1.readAs[String] should be ("A")
            message2.channel should be ("CHANNEL2")
            message2.readAs[String] should be ("C")
            message3.channel should be ("CHANNEL1")
            message3.readAs[String] should be ("D")
            message4.channel should be ("CHANNEL3")
            message4.readAs[String] should be ("E")
          }
        }

        unsubscribes should have size (3)
        clear()
      }
    }
  }

  PubSubRequests.PSubscribe.toString when {
    "some messages get published" should {
      Given("that the messages get published to non-matched channels")
      "not receive any messages" taggedAs (V200) in {
        client.pSubscribe("MyC*", "*5")(pf)
        Thread.sleep(200)
        publisher.publish("CHANNEL1", "A").futureValue should be (0)
        publisher.publish("CHANNEL2", "B").futureValue should be (0)
        publisher.publish("CHANNEL3", "C").futureValue should be (0)
        Thread.sleep(200)
        client.pUnsubscribe()
        Thread.sleep(200)
        pSubscribes.toList should contain theSameElementsAs List(
          PSubscribe("MyC*", 1),
          PSubscribe("*5", 2)
        )
        pMessages should be (empty)
        pUnsubscribes should have size (2)
        clear()
      }
      Given("that some messages get published to matching patterns")
      "receive the messages matching the patterns" taggedAs (V200) in {
        client.pSubscribe("MyC*", "*5")(pf)
        Thread.sleep(200)
        publisher.publish("MyChannel1", "A").futureValue should be (1)
        publisher.publish("CHANNEL3", "B").futureValue should be (0)
        publisher.publish("MyChannel2", "C").futureValue should be (1)
        publisher.publish("CHANNEL5", "D").futureValue should be (1)

        client.pSubscribe("*")(pf)
        Thread.sleep(200)
        publisher.publish("CHANNEL3", "E").futureValue should be (1)

        Thread.sleep(200)
        client.pUnsubscribe()
        Thread.sleep(200)

        pSubscribes.toList should contain theSameElementsAs List(
          PSubscribe("MyC*", 1),
          PSubscribe("*5", 2),
          PSubscribe("*", 3)
        )
        
        inside(pMessages.toList) {
          case List(message1, message2, message3, message4) => {
            message1.pattern should be ("MyC*")
            message1.channel should be ("MyChannel1")
            message1.readAs[String]() should be ("A")
            message2.pattern should be ("MyC*")
            message2.channel should be ("MyChannel2")
            message2.readAs[String]() should be ("C")
            message3.pattern should be ("*5")
            message3.channel should be ("CHANNEL5")
            message3.readAs[String]() should be ("D")
            message4.pattern should be ("*")
            message4.channel should be ("CHANNEL3")
            message4.readAs[String]() should be ("E")
          }
        }
        
        pUnsubscribes should have size (3)
        clear()
      }
    }
  }

  PubSubRequests.Unsubscribe.toString when {
    "unsubscribing from non-subscribed channels" should {
      "do nothing and still receive published messages" taggedAs (V200) in {
        client.subscribe("CHANNEL1", "CHANNEL2", "CHANNEL3")(pf)
        client.unsubscribe("CHANNEL0", "CHANNEL4")
        Thread.sleep(200)
        publisher.publish("CHANNEL1", "HELLO").futureValue should be (1)
        publisher.publish("CHANNEL2", "HELLO").futureValue should be (1)
        publisher.publish("CHANNEL3", "HELLO").futureValue should be (1)
        Thread.sleep(200)
        subscribes.toList should contain theSameElementsAs List(
          Subscribe("CHANNEL1", 1),
          Subscribe("CHANNEL2", 2),
          Subscribe("CHANNEL3", 3)
        )
        inside(messages.toList) {
          case List(message1, message2, message3) => {
            message1.channel should be ("CHANNEL1")
            message1.readAs[String] should be ("HELLO")
            message2.channel should be ("CHANNEL2")
            message2.readAs[String] should be ("HELLO")
            message3.channel should be ("CHANNEL3")
            message3.readAs[String] should be ("HELLO")
          }
        }
        unsubscribes should have size (2)
        unsubscribes.forall(_.channelsCount == 3) should be (true)
        clear()
      }
    }
    "unsubscribing from subscribed channels" should {
      "unsubscribe and published messages should no longer be received" taggedAs (V200) in {
        client.unsubscribe("CHANNEL1")
        Thread.sleep(200)
        publisher.publish("CHANNEL1", "HELLO").futureValue should be (0)
        publisher.publish("CHANNEL2", "HELLO").futureValue should be (1)
        publisher.publish("CHANNEL3", "HELLO").futureValue should be (1)
        Thread.sleep(200)
        client.unsubscribe()
        Thread.sleep(200)
        publisher.publish("CHANNEL1", "HELLO").futureValue should be (0)
        publisher.publish("CHANNEL2", "HELLO").futureValue should be (0)
        publisher.publish("CHANNEL3", "HELLO").futureValue should be (0)
        Thread.sleep(200)
        subscribes should be (empty)
        inside(messages.toList) {
          case List(message1, message2) => {
            message1.channel should be ("CHANNEL2")
            message1.readAs[String] should be ("HELLO")
            message2.channel should be ("CHANNEL3")
            message2.readAs[String] should be ("HELLO")
          }
        }
        unsubscribes should have size (3)
        clear()
      }
    }
  }

  PubSubRequests.PUnsubscribe.toString when {
    "unsubscribing from non-subscribed patterns" should {
      "do nothing and still receive published messages" taggedAs (V200) in {
        client.pSubscribe("*1", "*2", "*3")(pf)
        client.pUnsubscribe("*0", "*4")
        Thread.sleep(200)
        publisher.publish("CHANNEL1", "HELLO").futureValue should be (1)
        publisher.publish("CHANNEL2", "HELLO").futureValue should be (1)
        publisher.publish("CHANNEL3", "HELLO").futureValue should be (1)
        Thread.sleep(200)
        pSubscribes.toList should contain theSameElementsAs List(
          PSubscribe("*1", 1),
          PSubscribe("*2", 2),
          PSubscribe("*3", 3)
        )
        inside(pMessages.toList) {
          case List(message1, message2, message3) => {
            message1.pattern should be ("*1")
            message1.channel should be ("CHANNEL1")
            message1.readAs[String]() should be ("HELLO")
            message2.pattern should be ("*2")
            message2.channel should be ("CHANNEL2")
            message2.readAs[String]() should be ("HELLO")
            message3.pattern should be ("*3")
            message3.channel should be ("CHANNEL3")
            message3.readAs[String]() should be ("HELLO")
          }
        }
        pUnsubscribes should have size (2)
        pUnsubscribes.forall(_.patternsCount == 3) should be (true)
        clear()
      }
    }
    "unsubscribing from subscribed patterns" should {
      "unsubscribe and published messages should no longer be received" taggedAs (V200) in {
        client.pUnsubscribe("*1")
        Thread.sleep(200)
        publisher.publish("CHANNEL1", "HELLO").futureValue should be (0)
        publisher.publish("CHANNEL2", "HELLO").futureValue should be (1)
        publisher.publish("CHANNEL3", "HELLO").futureValue should be (1)
        Thread.sleep(200)
        client.pUnsubscribe()
        Thread.sleep(200)
        publisher.publish("CHANNEL1", "HELLO").futureValue should be (0)
        publisher.publish("CHANNEL2", "HELLO").futureValue should be (0)
        publisher.publish("CHANNEL3", "HELLO").futureValue should be (0)
        Thread.sleep(200)
        pSubscribes should be (empty)
        inside(pMessages.toList) {
          case List(message1, message2) => {
            message1.pattern should be ("*2")
            message1.channel should be ("CHANNEL2")
            message1.readAs[String]() should be ("HELLO")
            message2.pattern should be ("*3")
            message2.channel should be ("CHANNEL3")
            message2.readAs[String]() should be ("HELLO")
          }
        }
        pUnsubscribes should have size (3)
        clear()
      }
    }
  }

  override def afterAll() {
    publisher.quit().!
    client.quit()
    client2.quit()
    client3.quit()
    client4.quit()
  }
  
}