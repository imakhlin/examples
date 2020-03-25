import scala.collection._

import Dependencies._
import sbt.Keys._
import sbt._
//import sbtassembly.AssemblyPlugin.autoImport.assemblyOption

name := "examples"

version := "0.1"

scalaVersion := "2.13.1"

val buildVersion = sys.props.getOrElse("version", "0.0.1")

lazy val commonSettings = Defaults.itSettings ++ Vector(
  version := buildVersion,
  organization := "io.iguaz.v3io",
  scalaVersion := "2.11.12",
  testOptions in Test += Tests.Argument("-oGFD"),
  fork := true,
  credentials += Iguazio.credentials,
  resolvers += Iguazio.resolver,
  scalacOptions ++= List("-deprecation", "-feature", "-unchecked"),
  javacOptions ++= List("-source", "1.8"),
  publishArtifact in(Compile, packageDoc) := false,
  buildTask
)

lazy val IntegrationConfig = config("it") extend Test

lazy val `demo` = (project in file("demo"))
  .settings(commonSettings)
  .configs(IntegrationConfig)
  .settings(
    libraryDependencies ++= Seq(
      apacheHttpCore,
      apacheHttpClient,
      v3ioKeyValue % "provided",
      v3ioSpark2Stream % "provided",
      v3ioFile % "provided",
      v3ioObjectDataFrame % "it",
      v3ioHcfs % "it",
      sparkSql % "provided",
      sparkStreaming % "provided",
      scalaTest % "test,it",
      junit % "test,it",
      junitInterface % "test,it",
      easyMock % "test"))

val build = taskKey[Unit]("Test and build.")

val buildTask = build := {
  (test in Test).value
  assembly.?.value
  ()
}
