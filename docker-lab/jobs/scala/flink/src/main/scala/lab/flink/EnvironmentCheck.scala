package lab.flink

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

// Scala application using Flink's Java API: no Scala API macros are required.
object EnvironmentCheck {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromElements(Int.box(1), Int.box(2), Int.box(3))
      .map(new MapFunction[Integer, Integer] {
        override def map(value: Integer): Integer = Int.box(value.intValue() * 10)
      })
      .print()
    env.execute("scala-flink-environment-check")
  }
}
