# scredis

Scredis is a reactive, non-blocking and ultra-fast Scala [Redis](http://redis.io) client built on top of Akka IO. It has been (and still is) extensively used in production at Livestream.

* [Documentation](https://github.com/Livestream/scredis/wiki)
* [Scaladoc](http://livestream.github.io/scredis/api/snapshot/)

## Features
* Supports all Redis commands up to v2.8.13
* Built on top of Akka non-blocking IO
* Super fast, see Benchmarks section below
* Automatic reconnection
* Automatic pipelining
* Transactions
* Pub/Sub
  * Subscribe selectively with partial functions
  * Tracked Subscribe and Unsubscribe commands (they return a Future as any other commands)
  * Automatically resubscribes to previously subscribed channels/patterns upon reconnection
* Serialization and deserialization of command inputs and outputs
* Fully configurable
  * Akka dispatchers
  * Pipelined write batch size
  * Receive timeout
  * TCP buffer size hints
  * Request encoding buffer pool
  * Concurrent requests cap (bounded memory consumption)

## Getting started

### Binaries
Scredis 2.x.x is compatible with Scala 2.10 and 2.11. Binary releases are hosted on the Sonatype Central Repository.

```scala
resolvers += "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases/"

libraryDependencies += "com.livestream" %% "scredis" % "2.0.0-RC1"
```

Snapshots are hosted on a separate repository.

```scala
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

libraryDependencies += "com.livestream" %% "scredis" % "2.0.0-SNAPSHOT"
```

### Quick example
```scala
import scredis._
import scala.util.{ Success, Failure }
import scala.concurrent.ExecutionContext.Implicits.global

// Creates a Redis instance with default configuration.
// See reference.conf for the complete list of configurable parameters.
val redis = Redis()

// Executing a non-blocking command and registering callbacks on the returned Future
redis.hGetAll("my-hash") onComplete {
  case Success(content) => println(content)
  case Failure(e) => e.printStackTrace()
}

// Executes a blocking command using the internal, lazily initialized BlockingClient
redis.blocking.blPop(0, "queue")

// Subscribes to a Pub/Sub channel using the internal, lazily initialized SubscriberClient
redis.subscriber.subscribe("My Channel") {
  case message @ PubSubMessage.Message(channel, messageBytes) => println(
    message.readAs[String]()
  )
  case PubSubMessage.Subscribe(channel, subscribedChannelsCount) => println(
    s"Successfully subscribed to $channel"
  )
}

// Shutdown all initialized internal clients along with the ActorSystem
redis.quit()
```

## Benchmarks
Coming very soon!

## Scredis 1.x.x

### Binaries
Scredis 1.x.x is compatible with Scala 2.9.x, 2.10 and 2.11. Binary releases are hosted on the Sonatype Central Repository.

```scala
resolvers += "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases/"

libraryDependencies += "com.livestream" %% "scredis" % "1.1.2"
```

Snapshots are hosted on a separate repository.

```scala
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

libraryDependencies += "com.livestream" %% "scredis" % "1.1.2-SNAPSHOT"
```

## License

Copyright (c) 2013 Livestream LLC. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. See accompanying LICENSE file.
