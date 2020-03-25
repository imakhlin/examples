logLevel := Level.Warn

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.9.3")

// Prevents distracting SLF4J logs when SBT starts. These are caused by sbt-git's transitive dependency on slf4j-api.
libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.25"

