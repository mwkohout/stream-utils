val scala3Version = "3.2.0"

lazy val root = project
  .in(file("."))
  .settings(
    name := "stream-utils",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "0.7.29" % Test,
      "com.google.guava" % "guava" % "31.1-jre" % Test,
      "com.typesafe.akka" %% "akka-stream" % "2.6.20"
    )

  )

scalacOptions ++= Seq("-new-syntax", "-rewrite")

