name := "markovs"
organization := "io.andrewsmith"
version := "0.1"

scalaVersion := "2.13.3"

libraryDependencies ++= Seq(
  "co.fs2" %% "fs2-core" % "2.4.4",
  "co.fs2" %% "fs2-io" % "2.4.4",
  "org.apache.opennlp" % "opennlp-tools" % "1.9.3",
  "org.tpolecat" %% "doobie-core" % "0.9.0",
  "org.xerial" % "sqlite-jdbc" % "3.32.3"
)
