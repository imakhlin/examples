package io.iguaz.examples.demo

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.slf4j._

/**
  * Example:
  *
  * JDBC_DRIVER="/igz/demo/lib/mysql-connector-java-8.0.19/mysql-connector-java-8.0.19.jar"
  * V3IO_KV_API="/igz/java/libs/v3io-spark2-object-dataframe_2.11-#########.jar"
  *
  * spark-submit \
  * --driver-class-path "${JDBC_DRIVER}" \
  * --jars "${JDBC_DRIVER},${V3IO_KV_API}" \
  * --class io.iguaz.v3io.spark.streaming.demo.SelectJoinSaveKvDemoApp \
  * /igz/demo/lib/example.jar \
  * demo \
  * ${V3IO_ACCESS_KEY} \
  * "v3io://demo/demo-join-kv-table" \
  * "jdbc:mysql://mysql:3306/mlpipeline" \
  * "root" \
  * "" \
  * "com.mysql.jdbc.Driver" \
  * "SELECT * FROM pipelines"
  * "UUID"
  */
object SelectJoinSaveKvDemoApp {
  @transient private var log: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  private val defaultContainerName = "users"
  private val defaultKVTable = "v3io://demo/demo-join-kv-table"
  private val defaultAccessKey = "a7ef9cf5-9176-4129-99f8-ffc270d321f4" // admin's access key
  private val defaultJdbcUrl = "jdbc:mysql://mysql:3306/mlpipeline" // "jdbc:oracle:thin:@HOST:1521:ORCL"
  private val oracleJdbcDriver = "oracle.jdbc.driver.OracleDriver"
  private val mysqlJdbcDriver = "com.mysql.jdbc.Driver"
  private val defaultJdbcUser = "root"
  private val defaultJdbcPassword = ""
  private val defaultQuery = "SELECT 1 FROM DUAL;"
  private val defaultIDColumn = "UUID"

  // scalastyle:off method.length
  def main(args: Array[String]): Unit = {
    log.info(s"Input arguments: ${args.mkString(";")}")

    val spark = org.apache.spark.sql.SparkSession.builder
      .appName("Demo - select from RDBMS and store to KV example")
      .getOrCreate

    log.trace(s"Spark configuration: ${spark.conf.toString}")

    val inputArgs = parse(args)

    log.info(s"Starting example Select Join Save to KV application with $inputArgs")

    // Read from RDBMS
    val df: DataFrame = withTimer("Fetch from JDBC data source") {
      getDataFromRDBMS(spark.sqlContext, inputArgs)
    }

    // debug
    //df.show()

    // Write to V3IO KV
    withTimer("Store to V3IO KV") {
      df.write
        .format("io.iguaz.v3io.spark.sql.kv")
        .mode(SaveMode.Append) // or SaveMode.Overwrite
        .option("key", inputArgs.idColumn)
        .save(inputArgs.targetKVTable)
    }
  }

  private def parse(args: Array[String]) = {
    args.length match {
      case 1 => InputArguments(container = args(0))
      case 2 => InputArguments(container = args(0), accessKey = args(1)) //scalastyle:off magic.number
      case 3 => InputArguments(container = args(0), accessKey = args(1), //scalastyle:off magic.number
        targetKVTable = args(2))
      case 4 => InputArguments(container = args(0), accessKey = args(1), //scalastyle:off magic.number
        targetKVTable = args(2), jdbcUrl = args(3))
      case 5 => InputArguments(container = args(0), accessKey = args(1), //scalastyle:off magic.number
        targetKVTable = args(2), jdbcUrl = args(3), jdbcUser = args(4))
      case 6 => InputArguments(container = args(0), accessKey = args(1), //scalastyle:off magic.number
        targetKVTable = args(2), jdbcUrl = args(3), jdbcUser = args(4), jdbcPassword = args(5))
      case 7 => InputArguments(container = args(0), accessKey = args(1), //scalastyle:off magic.number
        targetKVTable = args(2), jdbcUrl = args(3), jdbcUser = args(4),
        jdbcPassword = args(5), jdbcDriverClass = args(6))
      case 8 => InputArguments(container = args(0), accessKey = args(1), //scalastyle:off magic.number
        targetKVTable = args(2), jdbcUrl = args(3), jdbcUser = args(4),
        jdbcPassword = args(5), jdbcDriverClass = args(6), query = args(7))
      case 9 => InputArguments(container = args(0), accessKey = args(1), //scalastyle:off magic.number
        targetKVTable = args(2), jdbcUrl = args(3), jdbcUser = args(4),
        jdbcPassword = args(5), jdbcDriverClass = args(6), query = args(7), idColumn = args(8))
      case _ => InputArguments() // all defaults
    }
  }

  def getDataFromRDBMS(sqlContext: SQLContext, params: InputArguments): DataFrame = {
    sqlContext
      .read
      .format("jdbc")
      .options(Map(
        "url" -> params.jdbcUrl,
        "user" -> params.jdbcUser,
        "password" -> params.jdbcPassword,
        "driver" -> params.jdbcDriverClass,
        "query" -> params.query))
      .load()
  }

  // Helper method
  def withTimer[R](blockName: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    val deltaInSeconds = (t1 - t0) / 1e9
    log.info(s"Elapsed time for block [$blockName]: $deltaInSeconds seconds.")
    result
  }

  case class InputArguments(container: String = defaultContainerName,
                            accessKey: String = defaultAccessKey,
                            targetKVTable: String = defaultKVTable,
                            jdbcUrl: String = defaultJdbcUrl,
                            jdbcUser: String = defaultJdbcUser,
                            jdbcPassword: String = defaultJdbcPassword,
                            jdbcDriverClass: String = mysqlJdbcDriver,
                            query: String = defaultQuery,
                            idColumn: String = defaultIDColumn
                           )

}
