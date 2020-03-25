package io.iguaz.examples.streaming

import io.iguaz.examples.utils.StreamingUtil

import io.iguaz.v3io.daemon.client.api.consts.ConfigProperty
import io.iguaz.v3io.streaming.StreamingOperationsFactory

object V3IOStreamProducer extends App {

  val defaultBatchSize = 10
  val defaultNumberOfShards: Short = 12
  val defaultPayloadSize = 1024

  if (args.length < 7) {
    println(
      s"""
         |Usage: V3IOStreamProducer <container-name> <duration (seconds)> <limit> <streams> <number-of-shards> <batch-size> <acknowledge> <payload-size>
         |  <container-name> V3IO container identifier
         |  <duration (seconds)> timeout in seconds
         |  <limit>  number of records to generate
         |  <streams> is a comma separated list of streams to produce records to (requires at least one stream)
         |           Note: stream name should start with leading slash, i.e. /someStream
         |  <number-of-shards> - the number of shards to create for non existing streams. Default is $defaultNumberOfShards
         |  <batch-size> - the number of records to generate at one (in single batch). Default is $defaultBatchSize
         |  <acknowledge> - wait for acknowledge for each put record request. Default is false
         |  <payload-size> - generate payload of given size (Bytes)
        """.stripMargin)
    System.exit(1)
  }

  val Array(containerId, durationSeconds, limit, streamNames, shardsCount, recordsPerBatch, doAcknowledge, payloadSize) = args

  val streamNameSet = streamNames.split(",").toSet
  val numberOfShards = Option(shardsCount) match {
    case Some(number) => number.toShort
    case None => defaultNumberOfShards
  }
  val batchSize = Option(recordsPerBatch) match {
    case Some(number) => number.toInt
    case None => defaultBatchSize
  }
  val acknowledge = Option(doAcknowledge) match {
    case Some(bool) => bool.toBoolean
    case None => false
  }
  val payloadSizeInBytes = Option(payloadSize) match {
    case Some(number) => number.toInt
    case None => defaultPayloadSize
  }

  println(
    s"""Input parameters:
       | container = $containerId
       | Duration: $durationSeconds Seconds
       | Streams = \'$streamNames\'
       | Stream names set: $streamNameSet
       | Shard count: $numberOfShards
       | Batch Size: $batchSize - the number of records to generate at once
       | Payload size: $payloadSizeInBytes Bytes"""
      .stripMargin)

  StreamingUtil.containerConfigProps.setProperty(ConfigProperty.CONTAINER_ID, containerId)

  val v3ioStreamingOperations = StreamingOperationsFactory.create(StreamingUtil.containerConfigProps)

  println(s"About to push records to the following streams: $streamNames at container $containerId")

  // Fetch stream & metadata from V3IO
  streamNameSet.foreach { tName =>
    // try to create stream if not exist
    StreamingUtil.createTopic(v3ioStreamingOperations, tName, numberOfShards)
    val streamDescriptor = v3ioStreamingOperations.describeStream(tName)
    println(s"Stream '$tName' has ${streamDescriptor.shardCount} shards")
  }

  val startTimeMs = System.currentTimeMillis()
  try {
    doProduce(durationSeconds.toInt, payloadSizeInBytes)
  } catch {
    case _: java.lang.InterruptedException => println("Interrupted by user.")
  } finally {
    println("\nDone")
  }

  /**
    * publish records to given streams
    */
  def doProduce(timeoutSeconds: Int, payloadSize: Int): Unit = {
    val recordsLimit = limit.toInt
    var totalRecordsSent = 0
    val payload = if (payloadSize > 0) {
      Some(scala.util.Random.alphanumeric.take(payloadSize).mkString)
    } else {
      None
    }

    do {
      totalRecordsSent += StreamingUtil.produceTestData(
        v3ioStreamingOperations,
        streamNameSet,
        numberOfShards,
        math.min(batchSize, recordsLimit - totalRecordsSent),
        acknowledge,
        payload,
        payloadSize)
      // wipe out the console and then print status line
      print("                                                                               ")
      print(s"\rProduced $totalRecordsSent records in total")
    } while (((System.currentTimeMillis() - startTimeMs) / 1000 < timeoutSeconds) && (totalRecordsSent < recordsLimit))

    // prevent program termination before all records have been pushed to the stream
    if (!acknowledge) {
      v3ioStreamingOperations.flush()
    }

    if ((System.currentTimeMillis() - startTimeMs) / 1000 >= timeoutSeconds) {
      println(s"\nFinished producing records due to $timeoutSeconds seconds timeout")
    }
    if (totalRecordsSent >= recordsLimit) {
      println(s"\nFinished producing $recordsLimit records")
    }
  }
}
