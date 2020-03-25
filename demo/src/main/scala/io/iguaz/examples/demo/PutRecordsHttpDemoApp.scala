package io.iguaz.examples.demo

import java.nio.charset.StandardCharsets
import java.util.Base64

import org.apache.http.HttpResponse
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.client.ResponseHandler
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.Row
import org.slf4j._

import io.iguaz.v3io.util.UriUtils

/**
  * Usage:
  * spark-submit \
  * --class io.iguaz.v3io.spark.streaming.demo.PutRecordsHttpDemoApp \
  * /full-path-to-the-jar.jar \
  * [CONTAINER_NAME] [STREAM_NAME] [SHARDS_COUNT] [PUT_BATCH_SIZE] [ACCESS_KEY] [RECORDS_TO_GENERATE]
  */
object PutRecordsHttpDemoApp {
  @transient private var log: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  private val defaultContainerName = "users"
  private val defaultStreamName = "/demo-put-records-stream"
  private val defaultShardsCount: Short = 12
  private val putRecordsMaxBatchSize = 1000
  private val defaultNumberOfRecords = 100000
  private val defaultAccessKey = "a7ef9cf5-9176-4129-99f8-ffc270d321f4" // admin's access key

  case class InputArguments(container: String = defaultContainerName,
                            stream: String = defaultStreamName,
                            shards: Short = defaultShardsCount,
                            putRecordsBatchSize: Int = putRecordsMaxBatchSize,
                            accessKey: String = defaultAccessKey,
                            recordsCount: Int = defaultNumberOfRecords)

  // scalastyle:off method.length
  def main(args: Array[String]): Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder
      .appName("Demo - Put Records example")
      .getOrCreate

    log.trace(s"Spark configuration: ${spark.conf.toString}")

    val inputArgs = args.length match {
      case 1 => InputArguments(container = args(0))
      case 2 => InputArguments(container = args(0), stream = args(1))
      case 3 => InputArguments(container = args(0), stream = args(1), shards = args(2).toShort)
      case 4 => InputArguments(container = args(0), stream = args(1), shards = args(2).toShort,
        putRecordsBatchSize = args(3).toInt)
      case 5 => InputArguments(container = args(0), stream = args(1), shards = args(2).toShort,
        putRecordsBatchSize = args(3).toInt, accessKey = args(4))
      case 6 => InputArguments(container = args(0), stream = args(1), shards = args(2).toShort,
        putRecordsBatchSize = args(3).toInt, accessKey = args(4), recordsCount = args(5).toInt)
    }

    log.info(s"Starting example PutRecords via HTTP application with $inputArgs")

    // Note, stream location should be absolute, i.e. start with "/"
    val streamPath = UriUtils.fromString(inputArgs.stream).getPath
    val url = s"http://v3io-webapi:8081/${inputArgs.container}$streamPath/"

    implicit val v3ioEndpoint = V3IOEndpoint(url, inputArgs.shards, inputArgs.accessKey)

    // create sample dataframe that mimics the dataframe received from Kafka stream
    import spark.implicits._
    val df = (1 to inputArgs.recordsCount)
      .map(i => (i.toString, i.toLong))
      .toDF("stringColumn", "longColumn");

    // Note, following block does not handle responses (foreach partition does not return response)
    //    withTimer(s"Put ${inputArgs.recordsCount} records to V3IO stream ${inputArgs.stream} using web api") {
    //      df.foreachPartition { rows =>
    //        import org.apache.http.impl.client.HttpClients
    //        implicit val httpClient = HttpClients.createDefault()
    //        withClosableHttpClient("Transform rows to records and put them to V3IO stream") {
    //          rows.grouped(putRecordsMaxBatchSize).foreach { batch =>
    //            sendBatchToV3IOStream(batch)
    //          }
    //        }
    //      }
    //    }

    while (true) {
      // Note, this block use mapPartitions and therefore it is capable of returning responses for further processing
      val responses =
        withTimer(s"Put ${inputArgs.recordsCount} records to V3IO stream ${inputArgs.stream} using web api") {
          df.mapPartitions { rows =>

            implicit val httpClient = HttpClients.createDefault()
            val responses = withClosableHttpClient("Transform rows to records and put them to V3IO stream") {
              rows.grouped(putRecordsMaxBatchSize).map { batch =>
                sendBatchToV3IOStream(batch)
              }
            }
            responses
          }
        }

      // print invalid responses (Http Status !=200)
      val resultDf = responses.filter(_ != 200)
      if (resultDf.count() > 0) {
        resultDf.show()
      }
    }
  }

  def withClosableHttpClient[R](blockName: String)(block: => R)(implicit httpClient: CloseableHttpClient): R = {
    log.info(s"In [$blockName]")
    block
    val result = block
    // Do not close http client since it will shut down connection manager and won't be able to serve next partition
    // Closing the http client results in illegal state exception!
    result
  }

  def sendBatchToV3IOStream(batch: Seq[Row])
                           (implicit v3ioEndpoint: V3IOEndpoint, httpClient: CloseableHttpClient): Int = {
    val records: Seq[Record] = batch.map(row => {
      val shardingKey = row.getAs[Long]("longColumn")
      val payload = Base64.getEncoder
        .encodeToString(row.getAs[String]("stringColumn").getBytes(StandardCharsets.UTF_8))
      val shardId = shardingKey % v3ioEndpoint.shardsCount
      Record("", payload, "", shardId.toShort)
    })

    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    val json =
    // stream path should not start with "/"; removing stream name from the body since it was added to the url
    // ("StreamName" -> v3ioEndpoint.streamPath) ~
      ("Records" -> records.map { record =>
        (("ClientInfo" -> record.ClientInfo) ~
          ("Data" -> record.Data) ~
          ("PartitionKey" -> record.PartitionKey) ~
          ("ShardId" -> record.ShardId))
      })
    val body = compact(render(json))
    val httpPut = new HttpPost(v3ioEndpoint.streamUrl)
    httpPut.setHeader("X-v3io-function", "PutRecords")
    httpPut.setHeader("X-v3io-session-key", v3ioEndpoint.accessKey)
    val stringEntity = new StringEntity(body, ContentType.APPLICATION_JSON)
    httpPut.setEntity(stringEntity)

    val responseHandler = new BasicResponseHandler()

    // Remove logs in production
    log.trace(s"PutRequest: ${httpPut.toString}")
    log.trace(s"PutRequest Headers: ${httpPut.getAllHeaders.map(h => s"${h.getName}=${h.getValue}").mkString("; ")}")
    log.trace(s"PutRequest Body: $body")
    val response = httpClient.execute(httpPut, responseHandler)
    response.status
  }

  def withTimer[R](blockName: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    val deltaInSeconds = (t1 - t0) / 1e9
    log.info(s"Elapsed time for block [$blockName]: $deltaInSeconds seconds.")
    result
  }

  case class V3IOEndpoint(streamUrl: String, shardsCount: Short, accessKey: String)

  case class Record(ClientInfo: String, Data: String, PartitionKey: String, ShardId: Short)

  case class Response(status: Int, message: String)

  private class BasicResponseHandler extends ResponseHandler[PutRecordsHttpDemoApp.Response] {
    override def handleResponse(response: HttpResponse): PutRecordsHttpDemoApp.Response = {
      //Get the status of the response
      response.getStatusLine().getStatusCode() match {
        case status if status >= 200 && status < 300 => Option(response.getEntity()) match {
          case Some(entity) => Response(status, EntityUtils.toString(entity))
          case None => Response(status, "")
        }
        case other =>
          log.trace(s"-> HTTP Status: $other; Reason: ${response.getStatusLine.getReasonPhrase}")
          Response(other, other.toString)
      }
    }
  }

}

