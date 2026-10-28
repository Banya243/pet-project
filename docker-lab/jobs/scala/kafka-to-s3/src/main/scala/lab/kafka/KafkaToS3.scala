package lab.kafka

import java.util.{Collection, Map => JMap}
import java.util.concurrent.TimeUnit

import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.PartitionOffsetsRetriever
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.{ParquetBuilder, ParquetWriterFactory}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, PartFileInfo}
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.TopicPartition
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.OutputFile

// Resolve the requested first-run position but never silently skip missing offsets on recovery.
final class StrictOffsetsInitializer(delegate: OffsetsInitializer) extends OffsetsInitializer {
  override def getPartitionOffsets(partitions: Collection[TopicPartition],
      retriever: PartitionOffsetsRetriever): JMap[TopicPartition, java.lang.Long] =
    delegate.getPartitionOffsets(partitions, retriever)

  override def getAutoOffsetResetStrategy: OffsetResetStrategy = OffsetResetStrategy.NONE
}

final class SizeAndCheckpointRollingPolicy(maxBytes: Long)
    extends CheckpointRollingPolicy[GenericRecord, String] {
  override def shouldRollOnEvent(file: PartFileInfo[String], event: GenericRecord): Boolean =
    file.getSize >= maxBytes
  override def shouldRollOnProcessingTime(file: PartFileInfo[String], now: Long): Boolean = false
}

object KafkaToS3 {
  def parquetWriter: ParquetWriterFactory[GenericRecord] =
    new ParquetWriterFactory[GenericRecord](new ParquetBuilder[GenericRecord] {
      override def createWriter(output: OutputFile): ParquetWriter[GenericRecord] =
        AvroParquetWriter.builder[GenericRecord](output)
          .withSchema(KafkaEnvelope.schema)
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .withRowGroupSize(32L * 1024L * 1024L)
          .build()
    })

  def main(args: Array[String]): Unit = {
    if (args.sameElements(Array("--help"))) {
      println(JobConfig.usage)
      return
    }
    val config = JobConfig.parse(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // Even the finite snapshot uses streaming checkpoint/commit semantics.
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    env.setParallelism(config.parallelism)
    env.setMaxParallelism(128)
    env.enableCheckpointing(config.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE)
    val checkpoints = env.getCheckpointConfig
    checkpoints.setCheckpointStorage(config.checkpointDir)
    checkpoints.setCheckpointTimeout(config.checkpointTimeoutMs)
    checkpoints.setMinPauseBetweenCheckpoints(Math.min(5000L, config.checkpointIntervalMs))
    checkpoints.setMaxConcurrentCheckpoints(1)
    checkpoints.setTolerableCheckpointFailureNumber(0)
    checkpoints.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
      Time.of(10, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))

    val initialOffsets = config.startingOffsets match {
      case "earliest" => OffsetsInitializer.earliest()
      case "latest" => OffsetsInitializer.latest()
      case "committed" => OffsetsInitializer.committedOffsets(OffsetResetStrategy.NONE)
    }
    val builder = KafkaSource.builder[GenericRecord]()
      .setProperties(JobConfig.kafkaProperties(config.kafkaPropertiesFile))
      .setBootstrapServers(config.bootstrapServers)
      .setTopics(config.topics: _*)
      .setGroupId(config.groupId)
      .setClientIdPrefix(config.groupId)
      .setStartingOffsets(new StrictOffsetsInitializer(initialOffsets))
      .setDeserializer(new EnvelopeDeserializer)
    if (config.bounded) builder.setBounded(OffsetsInitializer.latest())

    val sink = FileSink.forBulkFormat(new Path(config.output), parquetWriter)
      .withBucketAssigner(new TopicHourBucketAssigner)
      .withRollingPolicy(new SizeAndCheckpointRollingPolicy(config.maxFileSizeBytes))
      .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("part").withPartSuffix(".parquet").build())
      .build()

    env.fromSource(builder.build(), WatermarkStrategy.noWatermarks[GenericRecord](), "Kafka raw records")
      .uid("kafka-raw-source-v1")
      .sinkTo(sink)
      .name("S3 raw Parquet")
      .uid("s3-raw-parquet-v1")
    env.execute("kafka-to-s3-" + config.groupId)
  }
}
