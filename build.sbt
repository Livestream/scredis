import SiteKeys._
import GhPagesKeys._
import GitKeys._
import com.typesafe.sbt.SbtGhPages.ghpages

organization := "com.livestream"

name := "scredis"

version := "2.1.0-SNAPSHOT"

scalaVersion := "2.11.6"

scalacOptions ++= Seq("-deprecation")

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "com.typesafe" % "config" % "1.2.0",
  "com.typesafe.akka" %% "akka-actor" % "2.3.3",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.scalacheck" %% "scalacheck" % "1.12.2" % "test",
  "com.storm-enroute" %% "scalameter" % "0.6" % "test",
  "org.slf4j" % "slf4j-simple" % "1.7.12" % "test"
)

publishTo <<= version { (v: String) =>
  val repository = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at repository + "content/repositories/snapshots")
  else
    Some("releases" at repository + "service/local/staging/deploy/maven2")
}

// required so that test actor systems don't get messed up
fork in Test := true

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

scalacOptions += "-feature"

pomExtra := (
  <url>https://github.com/Livestream/scredis</url>
  <licenses>
    <license>
      <name>The Apache Software License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:Livestream/scredis.git</url>
    <connection>scm:git:git@github.com:Livestream/scredis.git</connection>
  </scm>
  <developers>
    <developer>
      <id>curreli</id>
      <name>Alexandre Curreli</name>
      <url>https://github.com/curreli</url>
    </developer>
  </developers>
)

parallelExecution in Test := false

testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")

concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

site.settings

ghpages.settings

siteMappings <++= (mappings in packageDoc in Compile, version) map { (m, v) =>
  for ((f, d) <- m) yield (
    f,
    if (v.trim.endsWith("SNAPSHOT")) ("api/snapshot/" + d) else ("api/"+v+"/"+d)
  )
}

synchLocal <<= (privateMappings, updatedRepository, gitRunner, streams) map { (mappings, repo, git, s) =>
  val betterMappings = mappings map { case (file, target) => (file, repo / target) }
  IO.copy(betterMappings)
  repo
}

git.remoteRepo := "git@github.com:Livestream/scredis.git"
