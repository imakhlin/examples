package io.iguaz.examples.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.streaming.v3io.V3IOUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Time}
import org.apache.spark.util.LongAccumulator
import io.iguaz.examples.utils.log.Logging
import io.iguaz.examples.utils.{Configuration, StreamingUtil}

import io.iguaz.v3io.daemon.client.api.consts.ConfigProperty
import io.iguaz.v3io.spark.streaming.{PayloadWithMetadata, RecordAndMetadata, StringDecoder}


object V3IOStreamProcessor extends Logging {
  // scalastyle:off
  def main(args: Array[String]): Unit = {
    val conf = new Configuration

    val enableBackpressure = conf.propConfig("spark.io.iguaz.examples.streaming.backpressure.enabled").getOrElse("False").toBoolean
    val readBufferSize = conf.propConfig("v3io.fs.read.buffer.size").getOrElse("65536")
    // Spark application configuration
    val batchDuration = conf.propConfig("spark.io.iguaz.examples.streaming.app.batch.duration").getOrElse("5000").toInt
    val limit = conf.propConfig("spark.io.iguaz.examples.streaming.app.limit.records").getOrElse("-1").toInt
    val cTimeUnits = conf.propConfig("spark.io.iguaz.examples.streaming.app.time.units").getOrElse("s")
    val topicsNames = conf.propConfig("spark.io.iguaz.examples.streaming.app.topic.names").getOrElse("undefined")
    val outputFile = conf.propConfig("spark.io.iguaz.examples.streaming.app.output.file.name").getOrElse("")
    val outputDataFrameCoalesce = conf.propConfig("spark.io.iguaz.examples.streaming.app.output.dataframe.coalesce")
      .getOrElse("1").toInt
    val showPerformanceMetrics = conf.propConfig("spark.io.iguaz.examples.streaming.app.output.show.performance")
      .getOrElse("True").toBoolean
    val topicNameSet = topicsNames.split(",").toSet

    StreamingUtil.setConfigurationParam(ConfigProperty.DEFAULT_DATA_BLOCK_SIZE, readBufferSize)

    logInfo(
      s"""Input parameters:
         |Batch duration (ms): $batchDuration
         |CTime units: $cTimeUnits
         |topics = $topicsNames
         |topic names set: ${topicNameSet.mkString(", ")}
         |Output file name: $outputFile
         |Read buffer size: $readBufferSize
         |Container config: ${StreamingUtil.containerConfigProps}
         |Initial Rate: ${conf.propConfig("spark.io.iguaz.examples.streaming.v3io.initialRatePerPartition").getOrElse("undefined")}
         |Max rate: ${conf.propConfig("spark.io.iguaz.examples.streaming.v3io.maxRatePerPartition").getOrElse("undefined")}
         |Back-pressure enabled: $enableBackpressure""".stripMargin)

    //============================================================================================================

    logInfo(s"About to process io.iguaz.examples.streaming data from the following topics: $topicsNames")
    val sparkConf = new SparkConf().setAppName("V3IOStreamProcessor")
      .set("spark.io.iguaz.examples.streaming.backpressure.enabled", enableBackpressure.toString)

    val ssc = new StreamingContext(sparkConf, Milliseconds(batchDuration))

    val stream = {
      val messageHandler = (rmd: RecordAndMetadata[String]) => rmd.payloadWithMetadata
      V3IOUtils.createDirectStream[String, StringDecoder, PayloadWithMetadata[String]](
        ssc,
        StreamingUtil.containerConfigMap,
        topicNameSet,
        messageHandler)
    }

    // Results accumulator
    val totalRecordsCount = ssc.sparkContext.longAccumulator("TotalRecordsCount")
    val totalBatchRecordsCount = ssc.sparkContext.longAccumulator("totalBatchRecordsCount")
    val totalProcessingRecordTimeMs = ssc.sparkContext.longAccumulator("TotalProcessingRecordTimeMs")

    val recordLogsByPublisher = withTimer("Make recordLogsByPublisher") {
      stream.map { payloadWithMetadata =>
        val topicName = payloadWithMetadata.topicName
        val shardId = payloadWithMetadata.partitionId

        logInfo(s"Topic: [$topicName] Shard: [$shardId]")

        // Note, when using NGINX to produce records,
        // the record's creation time units are (second: Long, nanosecond: Long)
        // When using java API, the units will be (millisecond: Long, nanosecond: Long)
        val cTimeMs = if ("s".equals(cTimeUnits)) {
          payloadWithMetadata.createTimeMs * 1000 + (payloadWithMetadata.createTimeNano / 1e6).toLong
        } else {
          payloadWithMetadata.createTimeMs
        }

        val processingTimeMs = System.currentTimeMillis()
        val processingDuration = processingTimeMs - cTimeMs

        // update accumulated processing time and total records count
        totalProcessingRecordTimeMs.add(processingDuration)
        totalRecordsCount.add(1)

        val k = ConsumerRecordKey(topicName, shardId)
        val v = AggregatedConsumerRecordLog(processingTimeMs, processingDuration, payloadWithMetadata.payload.length)
        (k, v)
      }
    }

    val aggWindowDuration = Milliseconds(batchDuration.toInt)
    val aggLogs = withTimer("recordLogsByPublisher.reduceByKeyAndWindow") {
      recordLogsByPublisher.reduceByKeyAndWindow(reduceAggregatedConsumerRecordLogs, aggWindowDuration)
    }

    // Convert RDDs of the record logs DStream to DataFrame and run SQL query
    val spark = SparkSession.builder().getOrCreate()
    aggLogs.foreachRDD { (rdd: RDD[(ConsumerRecordKey, AggregatedConsumerRecordLog)], time: Time) =>
      withTimer("aggLogs.foreachRDD") {
        import spark.implicits._

        // Convert RDD[(ConsumerRecordKey, AggregatedConsumerRecordLog)] to RDD[Tuple...] to DataFrame
        val logsDataFrame = withTimer("aggLogs.toDF") {
          rdd.map {
            case (ConsumerRecordKey(topicName, shardId),
            AggregatedConsumerRecordLog(ts, minDuration, maxDuration, avgDuration, payloadSize)) =>
              (ts, topicName, shardId, avgDuration, minDuration, maxDuration, payloadSize)
          }.toDF("timestamp", "topic", "partition", "duration_avg", "duration_min", "duration_max", "payload_size_avg")
        }

        logDebug(
          s"""Debug:
             |${rdd.toDebugString}
           """.stripMargin)

        logsDataFrame.cache()

        // Write aggregated batch data as parquet file for further aggregation by "summary" job
        if (outputFile != null && !outputFile.isEmpty) {
          if (outputFile.endsWith(".json")) {
            withTimer("Write to json") {
              logsDataFrame.write.mode(SaveMode.Append).json(outputFile)
            }
          } else {
            withTimer("Write to parquet") {
              logsDataFrame.coalesce(outputDataFrameCoalesce.toInt).write.mode(SaveMode.Append).parquet(outputFile)
            }
          }
        } else {
          logInfo("No output file supplied by user. Skipping report creation phase.")
          // do some action anyway
          withTimer("Count rows") {
            val rowsCount = logsDataFrame.count()
            logInfo(s"Rows count: $rowsCount")
          }
        }

        if (showPerformanceMetrics) {
          withTimer("Show Performance Metrics") {
            logsDataFrame.show()
          }
        }
      }
    }

    sys.ShutdownHookThread {
      logInfo("Gracefully stopping Spark Streaming Application")
      ssc.synchronized {
        ssc.notify()
      }
      var total = 0L
      // in some rare cases totalBatchRecordsCount > totalRecordsCount hence printing in total greater value
      if (totalRecordsCount.value > 0L && totalRecordsCount.value > totalBatchRecordsCount.value) {
        total = totalRecordsCount.value
      }
      else {
        total = totalBatchRecordsCount.value
      }
      logInfo(
        s"""Totals:
           |\tmessages received: $total
           |\taverage processing time (ms): ${totalProcessingRecordTimeMs.value / total}"""
          .stripMargin)

      logInfo("Application stopped")
      logInfo("\nDone")
    }

    ssc.addStreamingListener(new StreamingJobListener(ssc, stream.id, totalRecordsCount, limit.toInt))

    // Start the computation
    ssc.start()

    ssc.synchronized {
      ssc.wait()
    }
  }

  private def reduceAggregatedConsumerRecordLogs(rLog1: AggregatedConsumerRecordLog,
                                                 rLog2: AggregatedConsumerRecordLog) = {
    rLog1.copy(
      minProcessingDuration = math.min(rLog1.minProcessingDuration, rLog2.minProcessingDuration),
      maxProcessingDuration = math.max(rLog1.maxProcessingDuration, rLog2.maxProcessingDuration),
      avgProcessingDuration = (rLog1.avgProcessingDuration + rLog2.avgProcessingDuration) / 2,
      avgPayloadSize = (rLog1.avgPayloadSize + rLog2.avgPayloadSize) / 2
    )
  }

  def withTimer[T](timerName: String)(body: => T): T = {
    val startTime = System.currentTimeMillis()
    val result = body
    logInfo(s"\t~~~ $timerName -> ${System.currentTimeMillis() - startTime} ms.")
    result
  }
}

case class ConsumerRecordKey(topicName: String, shardId: Short)

case class AggregatedConsumerRecordLog(timestamp: Long,
                                       minProcessingDuration: Long,
                                       maxProcessingDuration: Long,
                                       avgProcessingDuration: Long,
                                       avgPayloadSize: Int)

object AggregatedConsumerRecordLog {
  def apply(timestamp: Long, duration: Long, payloadSize: Int): AggregatedConsumerRecordLog =
    AggregatedConsumerRecordLog(timestamp, duration, duration, duration, payloadSize)
}

case class AggregationResult(topicName: String,
                             shardId: Short,
                             avgProcessingDuration: Long,
                             minProcessingDuration: Long,
                             maxProcessingDuration: Long,
                             avgPayloadSize: Int) {

  override def toString: String = {
    s"""
       | Topic: $topicName Partition: $shardId
       | Processing Time: avg=$avgProcessingDuration min=$minProcessingDuration max=$maxProcessingDuration
       | Payload size: avg=$avgPayloadSize""".stripMargin
  }
}

private class StreamingJobListener(ssc: StreamingContext,
                                   streamId: Int,
                                   recordsReceived: LongAccumulator,
                                   limit: Long) extends StreamingListener with Logging {
  var shuttingDown = false
  var totalBatchRecordsCount: AtomicLong = new AtomicLong(0)
  var totalBatchesTimeMs: AtomicLong = new AtomicLong(0)

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = synchronized {
    val streamInputInfo = batchCompleted.batchInfo.streamIdToInputInfo.get(streamId)
    val metadata = streamInputInfo.map(_.metadata).getOrElse(Map())

    val numberOfRecordsInBatch = batchCompleted.batchInfo.numRecords
    val batchDuration = batchCompleted.batchInfo.processingDelay.getOrElse(0L)
    totalBatchRecordsCount.addAndGet(numberOfRecordsInBatch)
    totalBatchesTimeMs.addAndGet(batchDuration)

    val totalRecordsCount = recordsReceived.value
    if (limit > 0 && totalBatchRecordsCount.get >= limit) {
      logInfo("Reached the limit of records. Stopping the application.")

      if (!shuttingDown) {
        // TODO: find a better way to stop io.iguaz.examples.streaming context gracefully
        val pName: String = ManagementFactory.getRuntimeMXBean.getName
        val pid = pName.substring(0, pName.indexOf("@"))
        logInfo(s"Sending SIGINT to PID=$pid")
        val cmd = Array("/bin/bash", "-c", "kill -SIGINT " + pid)
        logInfo("Running " + cmd)
        val p: Process = Runtime.getRuntime.exec(cmd)
        val is = new BufferedReader(new InputStreamReader(p.getInputStream))
        val es = new BufferedReader(new InputStreamReader(p.getErrorStream))
        logInfo("[OUT]")
        var line = is.readLine()
        while (line != null) {
          logInfo(line)
          line = is.readLine()
        }
        logInfo("[END OUT]")
        logInfo("[ERR]")
        line = es.readLine()
        while (line != null) {
          logInfo(line)
          line = es.readLine()
        }
        logInfo("[END ERR]")
        val exitCode: Int = p.waitFor()
        logInfo("kill exit: " + exitCode)
        shuttingDown = true
      } else {
        logInfo(s"Shutdown in progress ...")
      }

      if (totalRecordsCount > 0L) {
        logInfo(
          s"""RDD statistics:
             | Messages received: $totalRecordsCount"""
            .stripMargin)
      }

      if (totalBatchRecordsCount.get() > 0) {
        logInfo(
          s"""Batch statistics:
             | Messages received: $totalBatchRecordsCount
             | Average processing time (ms per record): ${totalBatchesTimeMs.get / totalBatchRecordsCount.get}"""
            .stripMargin)

      }
    } else {
      logInfo(s"Total records count (in batch) is less than the limit: $totalBatchRecordsCount < $limit")
    }
  }
}
