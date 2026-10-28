package lab.kafka

import java.nio.ByteBuffer
import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.ArrayList

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord

object KafkaEnvelope {
  lazy val schema: Schema = {
    val input = getClass.getResourceAsStream("/kafka-envelope-v1.avsc")
    require(input != null, "Missing kafka-envelope-v1.avsc in application JAR")
    try new Schema.Parser().parse(input) finally input.close()
  }

  def fromKafka(record: ConsumerRecord[Array[Byte], Array[Byte]], ingestedAtMs: Long): GenericRecord = {
    def bytes(value: Array[Byte]): ByteBuffer = if (value == null) null else ByteBuffer.wrap(value)
    val envelope = new GenericData.Record(schema)
    envelope.put("schema_version", 1)
    envelope.put("topic", record.topic())
    envelope.put("partition", record.partition())
    envelope.put("offset", record.offset())
    envelope.put("kafka_timestamp_ms", record.timestamp())
    envelope.put("timestamp_type", record.timestampType().toString)
    envelope.put("ingested_at_ms", ingestedAtMs)
    envelope.put("key", bytes(record.key()))
    envelope.put("value", bytes(record.value()))
    val headerSchema = schema.getField("headers").schema().getElementType
    val headers = new ArrayList[GenericRecord]()
    val iterator = record.headers().iterator()
    while (iterator.hasNext) {
      val header = iterator.next()
      val result = new GenericData.Record(headerSchema)
      result.put("key", header.key())
      result.put("value", bytes(header.value()))
      headers.add(result)
    }
    envelope.put("headers", headers)
    envelope
  }
}

final class EnvelopeDeserializer extends KafkaRecordDeserializationSchema[GenericRecord] {
  @transient private var records: Counter = _
  @transient private var tombstones: Counter = _

  override def open(context: DeserializationSchema.InitializationContext): Unit = {
    records = context.getMetricGroup.counter("raw_records")
    tombstones = context.getMetricGroup.counter("raw_tombstones")
  }

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[GenericRecord]): Unit = {
    out.collect(KafkaEnvelope.fromKafka(record, System.currentTimeMillis()))
    records.inc()
    if (record.value() == null) tombstones.inc()
  }

  override def getProducedType: TypeInformation[GenericRecord] = new GenericRecordAvroTypeInfo(KafkaEnvelope.schema)
}

final class TopicHourBucketAssigner extends BucketAssigner[GenericRecord, String] {
  @transient private lazy val hour = DateTimeFormatter
    .ofPattern("'ingest_date='yyyy-MM-dd'/ingest_hour='HH").withZone(ZoneOffset.UTC)

  override def getBucketId(record: GenericRecord, context: BucketAssigner.Context): String = {
    val time = record.get("ingested_at_ms").asInstanceOf[Long]
    "topic=" + record.get("topic").toString + "/" + hour.format(Instant.ofEpochMilli(time))
  }

  override def getSerializer: SimpleVersionedSerializer[String] = SimpleVersionedStringSerializer.INSTANCE
}
