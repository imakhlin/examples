package io.iguaz.examples.utils

import java.security.MessageDigest
import java.util.Properties
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}

import scala.collection.JavaConversions.propertiesAsScalaMap
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.immutable.Map
import scala.collection.{Iterable, Set}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

import io.iguaz.v3io.container.streaming.AccumulativePutRecordCallback
import io.iguaz.v3io.streaming.StreamingOperations
import io.iguaz.v3io.streaming.api.ProducerRecord

object StreamingUtil {
  val defaultNumberOfPartitions: Short = 1
  val defaultRetentionTimeInHours: Duration = 24.hours
  val defaultPartitionSizeMb: Short = 16384
  val clientFactoryKey = "v3io.container.fs.client.factory"
  val defaultCallback = new AccumulativePutRecordCallback
  val parallelism: Int = sys.props.getOrElse("parallelism", "8").toInt

  private implicit val executor: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(parallelism,
      new ThreadFactory {
        override def newThread(r: Runnable): Thread = {
          val t: Thread = Executors.defaultThreadFactory.newThread(r)
          t.setName("v3io-stream-producer")
          t.setDaemon(true)
          t
        }
      }))

  private lazy val defaultConfigMap: scala.collection.immutable.Map[String, String] = {

    Map.empty[String, String]

  }

  val containerConfigProps: Properties = toProperties(defaultConfigMap)

  private def toProperties(map: Map[String, String]): Properties = {
    val p = new Properties
    p.putAll(map.asJava)
    p
  }

  def containerConfigMap: Map[String, String] = {
    propertiesAsScalaMap(containerConfigProps).toMap
  }

  def setConfigurationParam(key: String, value: String): AnyRef = {
    containerConfigProps.put(key, value)
  }

  def createTopic(streamingOperations: StreamingOperations, topicName: String,
                  numberOfPartitions: Short = defaultNumberOfPartitions,
                  retentionTimeInHours: Duration = defaultRetentionTimeInHours): Unit = {
    streamingOperations.createTopic(topicName, numberOfPartitions, retentionTimeInHours)
  }

  def produceTestData(streamingOperations: StreamingOperations, topicNames: Set[String],
                      numberOfPartitions: Short,
                      numberOfRecordsToGenerateAtOnce: Int,
                      acknowledge: Boolean,
                      payloadSize: Int): Long = {
    produceTestData(streamingOperations, topicNames, numberOfPartitions, numberOfRecordsToGenerateAtOnce,
      acknowledge, None, payloadSize)
  }

  def produceTestData(streamingOperations: StreamingOperations, topicNames: Set[String] = Set("/basic1"),
                      numberOfPartitions: Short = defaultNumberOfPartitions,
                      numberOfRecordsToGenerateAtOnce: Int = 10,
                      acknowledge: Boolean = false,
                      payloadOption: Option[String] = None,
                      payloadSize: Int = 1024): Int = withTimer {
    val payload = payloadOption match {
      case Some(data) => data
      case None if payloadSize > 0 => s"->${scala.util.Random.alphanumeric.take(payloadSize - 2).mkString}"
      case None => ""
    }
    val recordsRange = 1 to numberOfRecordsToGenerateAtOnce
    val records = recordsRange.foldLeft(Array[Array[Byte]]())((arr, index) => arr :+ s"$index$payload".getBytes)
    val recordsToTopicMap = topicNames.iterator.map(_ -> records.toIterable).toMap

    putRecords(streamingOperations, recordsToTopicMap, numberOfPartitions, acknowledge)
  }

  /** Send the messages to the V3IO */
  def putRecords(streamingOperations: StreamingOperations, recordsToTopicMap: Map[String, Iterable[Array[Byte]]],
                 numberOfPartitions: Short = defaultNumberOfPartitions, acknowledge: Boolean = false): Int = {

    // Note, use MD5 algorithm to generate unique key for the record based on it's message
    // TODO: add hashing algorithm property as producer configuration option,
    // i.e. v3io.io.iguaz.examples.streaming.partition.key.hash.algorithm=MD5
    val md5digest = MessageDigest.getInstance("MD5")
    var totalRecordsSent = 0

    // for each topic create the topic and publish the records associated with it
    recordsToTopicMap foreach {
      case (topicName, recordSet) =>
        val producesRecordsCount: Long = recordSet.size.toLong
        // since the partition ID is specified explicitly, the record key isn't required
        val callback: Option[PutRecordsCallback] = if (acknowledge) {
          Some(new PutRecordsCallback())
        } else {
          None
        }
        val futureSeq = recordSet.zipWithIndex.map { data =>
          val producerRecord = new ProducerRecord(
            topicName,
            (data._2 % numberOfPartitions).toShort,
            md5digest.digest(data._1),
            data._1,
            "ci".getBytes())

          val putRecordCompleteFuture: Future[Unit] = Future {
            streamingOperations.putRecord(producerRecord, callback.orNull)
          }
          putRecordCompleteFuture
        }
        //streamingOperations.flush()
        val results = Await.result(Future.sequence(futureSeq), Duration.Inf)
        totalRecordsSent += results.size

        callback.foreach { cb =>
          cb.waitForResponses(producesRecordsCount, PutRecordsCallback.DEFAULT_TIMEOUT_MS * producesRecordsCount)
        }
    }
    totalRecordsSent
  }

  def withTimer[T](body: => T, label: String = "no-name"): T = {
    val startTimeNano = System.nanoTime()
    val result = body
    val durationTimeNano = FiniteDuration(System.nanoTime() - startTimeNano, TimeUnit.NANOSECONDS)
    println(s"'$label' execution time: ${durationTimeNano.toMillis} [ms]")
    result
  }
}
