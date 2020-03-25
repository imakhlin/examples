package io.iguaz.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.v3io.V3IOUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import io.iguaz.examples.utils.StreamingUtil
import io.iguaz.examples.utils.log.Logging

import io.iguaz.v3io.daemon.client.api.consts.ConfigProperty
import io.iguaz.v3io.spark.streaming.{RecordAndMetadata, StringDecoder}
import io.iguaz.v3io.streaming.StreamingOperationsFactory
import io.iguaz.v3io.streaming.api.Topic


object V3IOStreamWordCount extends App with Logging {

  if (args.length < 2) {
    logError(
      s"""
         |Usage: V3IODirectStreamWordCount <container-name> <topics>
         |  <topics> is a comma separated list of topics to consume from (at least one topic required)
         |           Note: topic name should start with leading slash, i.e. /someTopic
        """.stripMargin)
    System.exit(1)
  }

  val Array(containerId, topicsNames) = args
  val topicNameSet = topicsNames.split(",").toSet

  logInfo(s"Input parameters:\n\tcontainer = $containerId\n\ttopics = \'$topicsNames\'\n\ttopic names set: " +
    s"$topicNameSet")

  StreamingUtil.setConfigurationParam(ConfigProperty.CONTAINER_ID, containerId)

  val v3ioStreamingOperations = StreamingOperationsFactory.create(StreamingUtil.containerConfigProps)

  logInfo(s"About to process io.iguaz.examples.streaming data from the following topics: $topicsNames at container $containerId")
  val sparkConf = new SparkConf().setAppName("V3IODirectStreamWordCount")
  // scalastyle:off magic.number
  val ssc = new StreamingContext(sparkConf, Milliseconds(50))

  // Fetch topic & metadata from V3IO
  val topics: Set[Topic] = topicNameSet.map { tName =>
    StreamingUtil.createTopic(v3ioStreamingOperations, tName)
    v3ioStreamingOperations.getTopic(tName)
  }

  val stream = {
    val messageHandler = (rmd: RecordAndMetadata[String]) => rmd.payload
    V3IOUtils.createDirectStream[String, StringDecoder, String](
      ssc,
      StreamingUtil.containerConfigMap,
      topicNameSet,
      messageHandler)
  }

  StreamingUtil.produceTestData(v3ioStreamingOperations, topicNameSet)

  // Get the lines, split them into words, count the words and print
  val words = stream.flatMap(_.split(" "))
  val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
  wordCounts.print()

  // Start the computation
  ssc.start()

  // publish even more records while stream is active
  (1 until 10) foreach { _ =>
    StreamingUtil.produceTestData(v3ioStreamingOperations, topicNameSet)
    Thread sleep 20
  }

  ssc.stop(stopSparkContext = true, stopGracefully = true)
  ssc.awaitTerminationOrTimeout(30000)
}
