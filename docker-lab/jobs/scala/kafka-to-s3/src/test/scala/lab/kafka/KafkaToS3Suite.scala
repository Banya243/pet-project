package lab.kafka

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.time.Instant
import java.util.Optional

import scala.collection.JavaConverters._

import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetResetStrategy}
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopInputFile

class KafkaToS3Suite extends munit.FunSuite {
  private val baseArgs = Array("--bootstrap-servers", "kafka:9092", "--topics", "orders.events",
    "--group-id", "orders-raw-v1", "--output", "s3a://raw/kafka/orders-v1",
    "--checkpoint-dir", "s3a://processed/flink/checkpoints/orders-raw-v1")
  private val instant = Instant.parse("2026-10-28T00:00:00Z").toEpochMilli

  private def kafkaRecord(value: Array[Byte], offset: Long = 42L): ConsumerRecord[Array[Byte], Array[Byte]] = {
    val headers = new RecordHeaders()
    headers.add("trace", Array[Byte](0, -1))
    headers.add("trace", null)
    new ConsumerRecord[Array[Byte], Array[Byte]]("orders.events", 1, offset, 1234L,
      TimestampType.CREATE_TIME, 0, if (value == null) -1 else value.length,
      Array.emptyByteArray, value, headers, Optional.empty[Integer]())
  }

  private def bytes(value: Any): List[Byte] = {
    val buffer = value.asInstanceOf[ByteBuffer].duplicate()
    val array = new Array[Byte](buffer.remaining())
    buffer.get(array)
    array.toList
  }

  test("configuration defaults and explicit finite mode") {
    val config = JobConfig.parse(baseArgs)
    assertEquals(config.startingOffsets, "earliest")
    assertEquals(config.checkpointIntervalMs, 30000L)
    assertEquals(config.parallelism, 2)
    assert(!config.bounded)
    assert(JobConfig.parse(baseArgs ++ Array("--bounded", "true")).bounded)
    assertEquals(JobConfig.kafkaProperties(None).getProperty("isolation.level"), "read_committed")
  }

  test("reject ambiguous options and unsafe output/checkpoint combinations") {
    intercept[IllegalArgumentException](JobConfig.parse(baseArgs ++ Array("--unknown", "value")))
    intercept[IllegalArgumentException](JobConfig.parse(baseArgs ++ Array("--parallelism", "0")))
    intercept[IllegalArgumentException](JobConfig.parse(baseArgs ++ Array("--group-id", "duplicate")))
    intercept[IllegalArgumentException](JobConfig.parse(baseArgs ++ Array("--bounded", "yes")))
    intercept[IllegalArgumentException](JobConfig.parse(baseArgs.dropRight(2) ++
      Array("--checkpoint-dir", "s3://raw/kafka/orders-v1/state")))
    intercept[IllegalArgumentException](JobConfig.parse(baseArgs.dropRight(2) ++
      Array("--checkpoint-dir", "s3a://processed/a/../state")))
    intercept[IllegalArgumentException](JobConfig.parse(baseArgs.dropRight(2) ++
      Array("--checkpoint-dir", "s3a://processed/")))
    intercept[IllegalArgumentException](JobConfig.parse(baseArgs ++ Array("--checkpoint-timeout-seconds", "1")))
  }

  test("reject attempts to override correctness properties without logging secrets") {
    val path = Files.createTempFile("kafka-client", ".properties")
    try {
      Files.write(path, "enable.auto.commit=true\nsasl.jaas.config=secret\n".getBytes(UTF_8))
      val error = intercept[IllegalArgumentException](JobConfig.kafkaProperties(Some(path.toString)))
      assert(error.getMessage.contains("enable.auto.commit"))
      assert(!error.getMessage.contains("secret"))
    } finally Files.deleteIfExists(path)
  }

  test("preserve arbitrary bytes, empty keys, repeated headers, Kafka timestamp and tombstones") {
    val envelope = KafkaEnvelope.fromKafka(kafkaRecord(Array[Byte](0, -1, -2, 10)), instant)
    assertEquals(envelope.get("topic"), "orders.events")
    assertEquals(envelope.get("partition").asInstanceOf[Int], 1)
    assertEquals(envelope.get("offset").asInstanceOf[Long], 42L)
    assertEquals(envelope.get("kafka_timestamp_ms").asInstanceOf[Long], 1234L)
    assertEquals(bytes(envelope.get("key")), Nil)
    assertEquals(bytes(envelope.get("value")), List[Byte](0, -1, -2, 10))
    val headers = envelope.get("headers").asInstanceOf[java.util.List[GenericRecord]].asScala
    assertEquals(headers.map(_.get("key")).toList, List("trace", "trace"))
    assertEquals(bytes(headers.head.get("value")), List[Byte](0, -1))
    assertEquals(headers(1).get("value"), null)
    assertEquals(KafkaEnvelope.fromKafka(kafkaRecord(null), instant).get("value"), null)
    assertEquals(bytes(KafkaEnvelope.fromKafka(kafkaRecord(Array.emptyByteArray), instant).get("value")), Nil)
  }

  test("explicit Avro serializer survives a Flink network round trip") {
    val record = KafkaEnvelope.fromKafka(kafkaRecord(Array[Byte](0, -1)), instant)
    val serializer = new GenericRecordAvroTypeInfo(KafkaEnvelope.schema).createSerializer(new ExecutionConfig())
    val output = new DataOutputSerializer(1024)
    serializer.serialize(record, output)
    val restored = serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer))
    assertEquals(bytes(restored.get("value")), List[Byte](0, -1))
    assertEquals(restored.get("offset").asInstanceOf[Long], 42L)
  }

  test("bucket by ingestion hour in UTC, including the day boundary") {
    val assigner = new TopicHourBucketAssigner
    assertEquals(assigner.getBucketId(KafkaEnvelope.fromKafka(kafkaRecord(null), instant), null),
      "topic=orders.events/ingest_date=2026-10-28/ingest_hour=00")
    assertEquals(assigner.getBucketId(KafkaEnvelope.fromKafka(kafkaRecord(null), instant - 1), null),
      "topic=orders.events/ingest_date=2026-10-27/ingest_hour=23")
  }

  test("strict recovery never automatically resets missing offsets") {
    assertEquals(new StrictOffsetsInitializer(OffsetsInitializer.earliest()).getAutoOffsetResetStrategy,
      OffsetResetStrategy.NONE)
    assertEquals(new StrictOffsetsInitializer(OffsetsInitializer.committedOffsets()).getAutoOffsetResetStrategy,
      OffsetResetStrategy.NONE)
  }

  test("write and read real Snappy Parquet with binary values and a tombstone") {
    val dir = Files.createTempDirectory("kafka-parquet")
    val path = dir.resolve("part.parquet")
    try {
      val output = FileSystem.getLocalFileSystem.create(new Path(path.toUri), FileSystem.WriteMode.NO_OVERWRITE)
      try {
        val writer = KafkaToS3.parquetWriter.create(output)
        writer.addElement(KafkaEnvelope.fromKafka(kafkaRecord(Array[Byte](0, -1), 42L), instant))
        writer.addElement(KafkaEnvelope.fromKafka(kafkaRecord(null, 43L), instant))
        writer.finish()
      } finally output.close()
      val hadoopPath = new org.apache.hadoop.fs.Path(path.toUri)
      val reader = AvroParquetReader.builder[GenericRecord](hadoopPath).build()
      try {
        assertEquals(bytes(reader.read().get("value")), List[Byte](0, -1))
        assertEquals(reader.read().get("value"), null)
        assertEquals(reader.read(), null)
      } finally reader.close()
      val file = ParquetFileReader.open(HadoopInputFile.fromPath(hadoopPath, new org.apache.hadoop.conf.Configuration()))
      try {
        val columns = file.getFooter.getBlocks.asScala.flatMap(_.getColumns.asScala)
        assert(columns.nonEmpty)
        assert(columns.forall(_.getCodec == CompressionCodecName.SNAPPY))
      } finally file.close()
    } finally {
      Files.deleteIfExists(dir.resolve(".part.parquet.crc"))
      Files.deleteIfExists(path)
      Files.deleteIfExists(dir)
    }
  }
}
