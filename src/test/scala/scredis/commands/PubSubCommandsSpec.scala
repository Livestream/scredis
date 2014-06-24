package scredis.commands

import org.scalatest.{ WordSpec, GivenWhenThen, BeforeAndAfterAll }
import org.scalatest.matchers.MustMatchers._

import scredis.{ Redis, Client }
import scredis.pubsub._
import scredis.exceptions.RedisCommandException
import scredis.tags._

import scala.collection.mutable.ListBuffer
import java.util.concurrent.{ CountDownLatch, TimeUnit }

class PubSubCommandsSpec extends WordSpec with GivenWhenThen with BeforeAndAfterAll {
  private val redis = Redis()
  private val client = Client()
  private val client2 = Client()
  private val client3 = Client()
  private val client4 = Client()
  private val SomeValue = "HelloWorld!虫àéç蟲"

  val subscribes = ListBuffer[Subscribe]()
  val unsubscribes = ListBuffer[Unsubscribe]()
  val messages = ListBuffer[Message]()

  val pSubscribes = ListBuffer[PSubscribe]()
  val pUnsubscribes = ListBuffer[PUnsubscribe]()
  val pMessages = ListBuffer[PMessage]()

  val pf: PartialFunction[PubSubMessage, Unit] = {
    case m: Subscribe => subscribes += m
    case m: Message => messages += m
    case m: Unsubscribe => unsubscribes += m
    case m: PSubscribe => pSubscribes += m
    case m: PMessage => pMessages += m
    case m: PUnsubscribe => pUnsubscribes += m
    case Error(e) => e.printStackTrace()
  }

  private def clear(): Unit = {
    subscribes.clear()
    unsubscribes.clear()
    messages.clear()
    pSubscribes.clear()
    pUnsubscribes.clear()
    pMessages.clear()
  }
  
  import scredis.util.TestUtils._

  Names.Publish when {
    "there are no clients subscribed to the channel" should {
      "return 0" taggedAs (V200) in {
        redis.publish("CHANNEL", "MESSAGE") must be(0)
        client.publish("CHANNEL", "MESSAGE") must be(0)
      }
    }
    "some clients have subscribed to the channel" should {
      "return the number of subscribed clients that received the message" taggedAs (V200) in {
        client2.subscribe("CHANNEL") {
          case Message(channel, message) =>
        }
        client3.subscribe("CHANNEL") {
          case Message(channel, message) =>
        }
        client.publish("CHANNEL", SomeValue) must be(2)
        client2.unsubscribe()
        client3.unsubscribe()
      }
    }
  }

  Names.Subscribe when {
    "some messages get published" should {
      Given("that the messages get published to non-subscribed channels")
      "not receive any messages" taggedAs (V200) in {
        client.subscribe("CHANNEL1", "CHANNEL2")(pf)
        client2.publish("CHANNEL3", "A")
        client3.publish("CHANNEL3", "B")
        client2.publish("CHANNEL4", "C")
        Thread.sleep(500)
        client.unsubscribe()
        Thread.sleep(500)
        subscribes.toList must be(List(Subscribe("CHANNEL1", 1), Subscribe("CHANNEL2", 2)))
        messages must be('empty)
        unsubscribes must have size (2)
        clear()
        assert(client.isSubscribed == false)
      }
      Given("that some messages get published to a subscribed channel")
      "receive the messages belonging to the subscribed channels" taggedAs (V200) in {
        client.subscribe("CHANNEL1", "CHANNEL2")(pf)
        client2.publish("CHANNEL1", "A")
        client3.publish("CHANNEL3", "B")
        client4.publish("CHANNEL2", "C")
        client2.publish("CHANNEL1", "D")

        client.subscribe("CHANNEL3")(pf)
        client3.publish("CHANNEL3", "E")

        Thread.sleep(500)
        client.unsubscribe()
        Thread.sleep(500)

        subscribes.toList must be(List(
          Subscribe("CHANNEL1", 1), Subscribe("CHANNEL2", 2), Subscribe("CHANNEL3", 3)))

        messages.toList must be(List(
          Message("CHANNEL1", "A"), Message("CHANNEL2", "C"),
          Message("CHANNEL1", "D"), Message("CHANNEL3", "E")))

        unsubscribes must have size (3)
        clear()
        assert(client.isSubscribed == false)
      }
    }
  }

  Names.PSubscribe when {
    "some messages get published" should {
      Given("that the messages get published to non-matched channels")
      "not receive any messages" taggedAs (V200) in {
        client.pSubscribe("MyC*", "*5")(pf)
        client2.publish("CHANNEL1", "A")
        client3.publish("CHANNEL2", "B")
        client2.publish("CHANNEL3", "C")
        Thread.sleep(500)
        client.pUnsubscribe()
        Thread.sleep(500)
        pSubscribes.toList must be(List(PSubscribe("MyC*", 1), PSubscribe("*5", 2)))
        pMessages must be('empty)
        pUnsubscribes must have size (2)
        clear()
      }
      Given("that some messages get published to matching patterns")
      "receive the messages matching the patterns" taggedAs (V200) in {
        client.pSubscribe("MyC*", "*5")(pf)
        client2.publish("MyChannel1", "A")
        client3.publish("CHANNEL3", "B")
        client4.publish("MyChannel2", "C")
        client2.publish("CHANNEL5", "D")

        client.pSubscribe("*")(pf)
        client3.publish("CHANNEL3", "E")

        Thread.sleep(500)
        client.pUnsubscribe()
        Thread.sleep(500)

        pSubscribes.toList must be(List(
          PSubscribe("MyC*", 1), PSubscribe("*5", 2), PSubscribe("*", 3)))

        pMessages.toList must be(List(
          PMessage("MyC*", "MyChannel1", "A"), PMessage("MyC*", "MyChannel2", "C"),
          PMessage("*5", "CHANNEL5", "D"), PMessage("*", "CHANNEL3", "E")))

        pUnsubscribes must have size (3)
        clear()
      }
    }
  }

  Names.Unsubscribe when {
    "unsubscribing from non-subscribed channels" should {
      "do nothing and still receive published messages" taggedAs (V200) in {
        client.subscribe("CHANNEL1", "CHANNEL2", "CHANNEL3")(pf)
        client.unsubscribe("CHANNEL0", "CHANNEL4")
        client2.publish("CHANNEL1", "HELLO")
        client3.publish("CHANNEL2", "HELLO")
        client4.publish("CHANNEL3", "HELLO")
        Thread.sleep(500)
        subscribes.toList must be(List(
          Subscribe("CHANNEL1", 1), Subscribe("CHANNEL2", 2), Subscribe("CHANNEL3", 3)))
        messages.toList must be(List(
          Message("CHANNEL1", "HELLO"), Message("CHANNEL2", "HELLO"), Message("CHANNEL3", "HELLO")))
        unsubscribes must have size (2)
        unsubscribes.forall(_.channelsCount == 3) must be(true)
        clear()
      }
    }
    "unsubscribing from subscribed channels" should {
      "unsubscribe and published messages should no longer be received" taggedAs (V200) in {
        client.unsubscribe("CHANNEL1")
        client2.publish("CHANNEL1", "HELLO")
        client3.publish("CHANNEL2", "HELLO")
        client4.publish("CHANNEL3", "HELLO")
        client.unsubscribe()
        client4.publish("CHANNEL1", "HELLO")
        client3.publish("CHANNEL2", "HELLO")
        client2.publish("CHANNEL3", "HELLO")
        subscribes must be('empty)
        messages.toList must be(List(
          Message("CHANNEL2", "HELLO"), Message("CHANNEL3", "HELLO")))
        unsubscribes must have size (3)
        clear()
      }
    }
  }

  Names.PUnsubscribe when {
    "unsubscribing from non-subscribed patterns" should {
      "do nothing and still receive published messages" taggedAs (V200) in {
        client.pSubscribe("*1", "*2", "*3")(pf)
        client.pUnsubscribe("*0", "*4")
        client2.publish("CHANNEL1", "HELLO")
        client3.publish("CHANNEL2", "HELLO")
        client4.publish("CHANNEL3", "HELLO")
        Thread.sleep(500)
        pSubscribes.toList must be(List(
          PSubscribe("*1", 1), PSubscribe("*2", 2), PSubscribe("*3", 3)))
        pMessages.toList must be(List(
          PMessage("*1", "CHANNEL1", "HELLO"),
          PMessage("*2", "CHANNEL2", "HELLO"),
          PMessage("*3", "CHANNEL3", "HELLO")))
        pUnsubscribes must have size (2)
        pUnsubscribes.forall(_.patternsCount == 3) must be(true)
        clear()
      }
    }
    "unsubscribing from subscribed patterns" should {
      "unsubscribe and published messages should no longer be received" taggedAs (V200) in {
        client.pUnsubscribe("*1")
        client2.publish("CHANNEL1", "HELLO")
        client3.publish("CHANNEL2", "HELLO")
        client4.publish("CHANNEL3", "HELLO")
        client.pUnsubscribe()
        client4.publish("CHANNEL1", "HELLO")
        client3.publish("CHANNEL2", "HELLO")
        client2.publish("CHANNEL3", "HELLO")
        pSubscribes must be('empty)
        pMessages.toList must be(List(
          PMessage("*2", "CHANNEL2", "HELLO"), PMessage("*3", "CHANNEL3", "HELLO")))
        pUnsubscribes must have size (3)
        clear()
      }
    }
  }

  override def afterAll() {
    redis.quit()
    
    client.unsubscribe()
    client.pUnsubscribe()

    client2.unsubscribe()
    client2.pUnsubscribe()

    client3.unsubscribe()
    client3.pUnsubscribe()

    client4.unsubscribe()
    client4.pUnsubscribe()

    client.quit()
    client2.quit()
    client3.quit()
    client4.quit()
  }

}