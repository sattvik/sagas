name := "sagas"

version := "0.1.0"

scalaVersion := "2.11.8"

val akkaVersion = "2.4.10"

libraryDependencies ++= Seq(
  "com.typesafe.akka"          %% "akka-stream"         % akkaVersion,
  "com.typesafe.scala-logging" %% "scala-logging"       % "3.4.0",
  "ch.qos.logback"              % "logback-classic"     % "1.1.7"     % Test,
  "com.typesafe.akka"          %% "akka-slf4j"          % akkaVersion % Test,
  "com.typesafe.akka"          %% "akka-stream-testkit" % akkaVersion % Test,
  "org.scalacheck"             %% "scalacheck"          % "1.13.2"    % Test,
  "org.scalatest"              %% "scalatest"           % "3.0.0"     % Test
)

dependencyOverrides ++= Set(
  "org.scala-lang.modules" %% "scala-xml" % "1.0.5"
)