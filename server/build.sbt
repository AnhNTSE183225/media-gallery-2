ThisBuild / scalaVersion := "3.3.3"
ThisBuild / organization := "mg2"
ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val http4sVersion = "0.23.30"
lazy val circeVersion = "0.14.9"

lazy val root = (project in file("."))
  .settings(
    name := "media-gallery-2-server",
    Compile / run / fork := true,
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "3.5.4",
      "org.http4s" %% "http4s-dsl" % http4sVersion,
      "org.http4s" %% "http4s-ember-server" % http4sVersion,
      "org.http4s" %% "http4s-circe" % http4sVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "io.circe" %% "circe-yaml" % "0.15.1",
      "org.postgresql" % "postgresql" % "42.7.4",
      "org.slf4j" % "slf4j-simple" % "2.0.13"
    )
  )
