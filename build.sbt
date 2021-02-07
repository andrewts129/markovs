import java.nio.file.Files
import java.nio.file.Path
import sys.process._

name := "markovs"
organization := "io.andrewsmith"
version := "0.1-SNAPSHOT"
githubOwner := "andrewts129"
githubRepository := "markovs"

scalaVersion := "2.13.3"

libraryDependencies ++= Seq(
  "co.fs2" %% "fs2-core" % "2.4.4",
  "co.fs2" %% "fs2-io" % "2.4.4",
  "org.apache.opennlp" % "opennlp-tools" % "1.9.3",
  "org.tpolecat" %% "doobie-core" % "0.9.0",
  "org.xerial" % "sqlite-jdbc" % "3.32.3",
  "com.monovore" %% "decline" % "1.3.0",
  "com.monovore" %% "decline-effect" % "1.3.0"
)

val downloadNlpModel = taskKey[Unit]("Downloads the OpenNLP model used for POS tagging")
downloadNlpModel := {
  val path = Path.of("src", "main", "resources", "en-pos-maxent.bin")
  if (!Files.exists(path)) {
    (url("http://opennlp.sourceforge.net/models-1.5/en-pos-maxent.bin") #> path.toFile).!!
  }
}

(Compile / compile) := ((Compile / compile) dependsOn downloadNlpModel).value
