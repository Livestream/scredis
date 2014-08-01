# scredis

Scredis is a reactive, non-blocking and ultra-fast Scala [Redis](http://redis.io) client built on top of Akka IO. It has been (and still is) extensively used in production at Livestream.

* [Documentation](https://github.com/Livestream/scredis/wiki)
* [Scaladoc](http://livestream.github.io/scredis/api/snapshot/)

## Features
* Supports all Redis commands up to v2.8.13
* Built on top of Akka non-blocking IO
* Super fast, see [Benchmarks](#benchmarks) section below
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

libraryDependencies += "com.livestream" %% "scredis" % "2.0.0"
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

The following benchmarks have been performed using [ScalaMeter](http://scalameter.github.io/) with the `SeparateJvmsExecutor`, configured with `Warmer.Default`, `Measurer.Default` and `Aggregator.average`. The source code can be found [here](https://github.com/Livestream/scredis/blob/master/src/test/scala/scredis/ClientBenchmark.scala).

### Hardware
* MacBook Pro (15-inch, Early 2011)
* 2.0GHz quad-core Intel Core i7 processor with 6MB shared L3 cache
* 16GB of 1333MHz DDR3 memory
* Mac OS X 10.9.4

### Java
```
> java -version
java version "1.7.0_45"
Java(TM) SE Runtime Environment (build 1.7.0_45-b18)
Java HotSpot(TM) 64-Bit Server VM (build 24.45-b08, mixed mode)
```

### Scala
Scala 2.11.2

### Scredis
2.0.0-RC1 with default configuration

### Redis
Redis 2.8.13 running locally (on the same machine)

### Results

```
[info] :::Summary of regression test results - Accepter():::
[info] Test group: Client.PING
[info] - Client.PING.Test-0 measurements:
[info]   - at size -> 1000000: passed
[info]     (mean = 1496.30 ms, ci = <1396.51 ms, 1596.10 ms>, significance = 1.0E-10)
[info]   - at size -> 2000000: passed
[info]     (mean = 3106.07 ms, ci = <2849.27 ms, 3362.87 ms>, significance = 1.0E-10)
[info]   - at size -> 3000000: passed
[info]     (mean = 4735.93 ms, ci = <4494.92 ms, 4976.94 ms>, significance = 1.0E-10)
[info]
[info] Test group: Client.GET
[info] - Client.GET.Test-1 measurements:
[info]   - at size -> 1000000: passed
[info]     (mean = 2452.47 ms, ci = <2308.81 ms, 2596.12 ms>, significance = 1.0E-10)
[info]   - at size -> 2000000: passed
[info]     (mean = 4880.42 ms, ci = <4629.75 ms, 5131.09 ms>, significance = 1.0E-10)
[info]   - at size -> 3000000: passed
[info]     (mean = 7271.20 ms, ci = <6795.45 ms, 7746.94 ms>, significance = 1.0E-10)
[info]
[info] Test group: Client.SET
[info] - Client.SET.Test-2 measurements:
[info]   - at size -> 1000000: passed
[info]     (mean = 2969.00 ms, ci = <2768.45 ms, 3169.54 ms>, significance = 1.0E-10)
[info]   - at size -> 2000000: passed
[info]     (mean = 5912.59 ms, ci = <5665.94 ms, 6159.24 ms>, significance = 1.0E-10)
[info]   - at size -> 3000000: passed
[info]     (mean = 8752.69 ms, ci = <8403.07 ms, 9102.31 ms>, significance = 1.0E-10)
[info]
[info]  Summary: 3 tests passed, 0 tests failed.
```

#### Ping
* 1,000,000 requests -> 1496.30 ms = 668,315 req/s
* 2,000,000 requests -> 3106.07 ms = 643,900 req/s
* 3,000,000 requests -> 4735.93 ms = 633,455 req/s

#### Get
* 1,000,000 requests -> 2452.47 ms = 407,752 req/s
* 2,000,000 requests -> 4880.42 ms = 409,801 req/s
* 3,000,000 requests -> 7271.20 ms = 412,587 req/s

#### Set
* 1,000,000 requests -> 2969.00 ms = 336,814 req/s
* 2,000,000 requests -> 5912.59 ms = 338,261 req/s
* 3,000,000 requests -> 8752.69 ms = 342,752 req/s


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
