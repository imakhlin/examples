import sbt._

object Dependencies {

  val apacheCommonsIO = "commons-io" % "commons-io" % "2.5"
  val apacheCommonsLang3 = "org.apache.commons" % "commons-lang3" % "3.4"
  val apacheHttpCore = "org.apache.httpcomponents" % "httpcore" % "4.4.4"
  val apacheHttpClient = "org.apache.httpcomponents" % "httpclient" % "4.5.2"

  //val json4sJackson = "org.json4s" %% "json4s-jackson" % "3.4.2"
  val json4sJackson = "org.json4s" %% "json4s-jackson" % "3.5.3"

  val slf4j = "org.slf4j" % "slf4j-api" % "1.7.25"
  val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"

  private val typesafeConfigVersion = "1.2.1"
  val typesafeConfig = "com.typesafe" % "config" % typesafeConfigVersion

  private val sparkVersion = "2.4.4"
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion
  val sparkStreaming = "org.apache.spark" %% "spark-streaming" % sparkVersion

  val scalatic = "org.scalactic" %% "scalactic" % "3.1.1"
  val scalaTest = "org.scalatest" %% "scalatest" % "3.1.1"
  val easyMock = "org.easymock" % "easymock" % "3.4"
  val junit = "junit" % "junit" % "4.12"

  // Enables SBT detection of JUnit tests
  val junitInterface = "com.novocode" % "junit-interface" % "0.11"

  // Zeta libraries
  val v3ioSpark2Stream = "io.iguaz.v3io" %% "v3io-spark2-streaming" % Iguazio.zetaVersion
  val v3ioKeyValue = "io.iguaz.v3io" %% "v3io-key-value" % Iguazio.zetaVersion
  val v3ioFile = "io.iguaz.v3io" %% "v3io-file" % Iguazio.zetaVersion
  val v3ioObjectDataFrame = "io.iguaz.v3io" %% "v3io-spark2-object-dataframe" % Iguazio.zetaVersion
  val v3ioHcfs = "io.iguaz.v3io" %% "v3io-hcfs" % Iguazio.zetaVersion
}

object Iguazio {
  val credentials = Credentials(
    "Artifactory Realm",
    "artifactory.iguazeng.com",
    "iguazio_ro",
    "AP6zvYnUy6UqWtgfE7uidNWhFF5"
  )

  val zetaVersion: String = {
    val defaultVersion = sys.props.getOrElse("default.version", "2.8_stable")
    sys.props.getOrElse("zeta.version", defaultVersion)
  }

  val resolver = ("Iguazio Artifactory" at "http://artifactory.iguazeng.com:8081/artifactory/libs-snapshot")
    .withAllowInsecureProtocol(true)
}
