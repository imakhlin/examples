package io.iguaz.examples.demo

import java.nio.charset.StandardCharsets
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import io.iguaz.examples.utils.log.Logging
import io.iguaz.v3io.api.exception.V3IOExecutionException
import io.iguaz.v3io.daemon.client.api.consts.{ConfigProperty, V3IOResultCode}
import io.iguaz.v3io.streaming.api.{ProducerRecord, TopicPartition, V3IOPutRecordCallback}
import io.iguaz.v3io.streaming.{Constants, StreamingOperationsFactory}
import io.iguaz.v3io.util.UriUtils

/**
 * Usage:
 * spark-submit \
 * --class io.iguaz.v3io.spark.streaming.demo.PutRecordsDemoApp \
 * /full-path-to-the-jar.jar [CONTAINER_NAME] [STREAM_NAME]
 */
object PutRecordsDemoApp extends Logging {
  private val defaultContainerName = "users"
  private val defaultStreamName = "/demo-put-records-stream"
  private val shardsCount: Short = 12
  private val streamRetention = Duration(24, TimeUnit.HOURS) // scalastyle:off magic.number
  // partition key length up to 256 Bytes
  private val defaultPartitionKey = Array.empty[Byte] //"some-key".getBytes(StandardCharsets.UTF_8)
  // client info length up to 16 Bytes
  private val defaultClientInfo = Array.empty[Byte] //"extra-bytes".getBytes(StandardCharsets.UTF_8)

  def main(args: Array[String]): Unit = {

    // parse input arguments
    val (containerName, pathToStream) = if (args.length == 1) {
      (args(0), defaultContainerName)
    } else if (args.length == 2) {
      (args(0), args(1))
    } else {
      (defaultContainerName, defaultStreamName)
    }

    // Note, stream location should be absolute, i.e. start with "/"
    val streamPath = UriUtils.fromString(pathToStream).getPath

    // create streaming operations interface
    val v3ioStreamingOperations = StreamingOperationsFactory.create(getConfig(containerName))

    // create new stream for testing purposes only
    v3ioStreamingOperations.createTopic(streamPath, shardsCount, streamRetention)

    // for each shard generate and publish number of records of certain size (Bytes)
    (0 until shardsCount)
      .foreach(shard => {
        getSomeRecordsFromKafka(streamPath, shard.toShort,
          numberOfRecords = 1024, payloadSize = 32).zipWithIndex // scalastyle:off magic.number
          .foreach { case (record, sequenceId) =>
            v3ioStreamingOperations.putRecord(record, PutRecordCustomCallback(sequenceId))
          }
      })
  }

  // provide default configuration - make it configurable
  def getConfig(container: String): Properties = {
    val containerProps = new Properties()
    containerProps.setProperty(ConfigProperty.CONTAINER_ID, container)

    // you can tune the following parameters to optimise behaviour for specific use case
    containerProps.setProperty(Constants.PutRecordAutoFlushInterval, "20 ms")
    containerProps.setProperty(Constants.PutRecordMaxBufferSize, "16 KiB")
    containerProps.setProperty(Constants.PutRecordBuffersPoolSize, "64")
    containerProps.setProperty(Constants.PutRecordAutoFlushRecords, "50")

    containerProps
  }

  // generate sample records with random content
  def getSomeRecordsFromKafka(streamName: String, partition: Short, numberOfRecords: Int, payloadSize: Int)
  : Seq[ProducerRecord] = {
    (0 to numberOfRecords).map(_ => {
      val payload = scala.util.Random.alphanumeric.take(payloadSize).mkString.getBytes(StandardCharsets.UTF_8)

      // note, payload, partition key and client info contains fake data for demo purpose only
      new ProducerRecord(new TopicPartition(streamName, partition), defaultPartitionKey, payload, defaultClientInfo)
    })
  }
}

// callback like this could be used for handling responses
case class PutRecordCustomCallback(index: Int) extends V3IOPutRecordCallback with Logging {
  val id: Int = index
  var recordSequenceId: Long = -1
  var recordShardId: Short = -1
  var success = false
  var returnCode: V3IOResultCode.Errors = V3IOResultCode.Errors.UNKNOWN_ERROR
  var active = true

  override def onFailure(errCode: V3IOResultCode.Errors): Unit = if (active) {
    active = errCode == V3IOResultCode.Errors.TRY_AGAIN
    returnCode = errCode
    success = false
    throw new V3IOExecutionException(s"Put record [$index] has failed with $errCode error code")
  } else {
    logError(s"onFailure: Attempt to modify an inactive callback. [$returnCode->$errCode]")
  }

  override def onSuccess(sequenceId: Long, shardId: Short): Unit = if (active) {
    active = false
    recordSequenceId = sequenceId
    recordShardId = shardId
    success = true
    returnCode = V3IOResultCode.Errors.OK
    logTrace(s"Put record [$index] succeeded. Record sequenceId=$sequenceId stored to shard $shardId")
  } else {
    logError(s"onSuccess: Attempt to modify an inactive callback. " +
      s"[$shardId->$shardId; $recordSequenceId->$sequenceId]")
  }
}

