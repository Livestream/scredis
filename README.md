# scredis

Scredis is an advanced [Redis](http://redis.io) client entirely written in Scala. It has been (and still is) extensively used in production at Livestream.

* [Documentation](https://github.com/Livestream/scredis/wiki)
* [Scaladoc](http://livestream.github.io/scredis/)

## Features
* Supports all Redis 2.6.x commands
* Native Scala types and customizable parsing
* Asynchronous and synchronous commands processing
* Transactions, pipelining and configurable automatic pipelining
* Customizable timeouts and retries on a per-command basis
* Client pooling

## Getting started

### Binaries
Scredis is compatible with Scala 2.9.x and 2.10.x. Binary releases will soon be hosted on a Nexus repository. This page will be updated once the repository is available.

```scala
// Comming soon!
resolvers += "Name" at "Releases URL"

libraryDependencies ++= Seq("livestream" %% "scredis" % "1.0.0")
```

Snapshots will be hosted in a separate repository.

```scala
// Comming soon!
resolvers += "Name" at "Snapshots URL"

libraryDependencies ++= Seq("livestream" %% "scredis" % "1.0.1-SNAPSHOT")
```

### Quick example
```scala
import scredis._
import scala.util.{ Success, Failure }
// Creates a rich asynchronous client with default parameters.
// See reference.conf for the complete list of configurable parameters.
val redis = Redis()

// Import the default execution context for working with futures
import redis.ec

// Futures handling
redis.hGetAll("my-hash") onComplete {
  case Success(content) => println(content)
  case Failure(e) => e.printStackTrace()
}

// Explicit pipelining
redis.pipelined { p =>
  p.hSet("my-hash")("name", "value")
  p.hGet("my-hash")("name")
}

// Executes a synchronous command, or any other command synchronously
redis.sync(_.blPop(0, "queue"))
redis.withClient(_.blPop(0, "queue"))

// Disconnects all internal clients and shutdown the default execution context
redis.quit()
```
