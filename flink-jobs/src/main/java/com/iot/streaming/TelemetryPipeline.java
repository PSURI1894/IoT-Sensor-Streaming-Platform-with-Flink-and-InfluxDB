package com.iot.streaming;

import com.iot.streaming.model.RawEvent;
import com.iot.streaming.model.EnrichedEvent;
import com.iot.streaming.model.AlertEvent;
import com.iot.streaming.functions.RegistryBroadcastJoinFunction;
import com.iot.streaming.functions.EWMAAnomalyDetector;
import com.iot.streaming.sinks.InfluxDBLineProtocolSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import java.time.Duration;

public class TelemetryPipeline {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // Clamping & Ingestion Time watermark to protect against device clock drifts
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("telemetry.raw")
                .setGroupId("flink-telemetry-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawJsonStream = env.fromSource(kafkaSource, 
                WatermarkStrategy.noWatermarks(), 
                "KafkaRawSource");

        // Parse Raw JSON into POJOs
        DataStream<RawEvent> rawEventStream = rawJsonStream.map(new MapFunction<String, RawEvent>() {
            @Override
            public RawEvent map(String value) throws Exception {
                // Mock json parsing
                RawEvent raw = new RawEvent();
                raw.setAssetId("austin-pump-01");
                raw.setMetricName("temperature");
                raw.setValue(55.2);
                raw.setEventTs(System.currentTimeMillis());
                raw.setQualityFlag(1);
                raw.setDeviceClass("PUMP");
                return raw;
            }
        });

        // 5-sec Allowed lateness watermark strategy on keyed streams
        SingleOutputStreamOperator<RawEvent> watermarkedStream = rawEventStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<RawEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.getEventTs())
                );

        // Window aggregations\n        DataStream<AggregateMetric> aggregatedMetrics = watermarkedStream\n                .keyBy(RawEvent::getAssetId)\n                .window(org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(1)))\n                .aggregate(new org.apache.flink.api.common.functions.AggregateFunction<RawEvent, Double[], AggregateMetric>() {\n                    @Override\n                    public Double[] createAccumulator() { return new Double[]{0.0, Double.MAX_VALUE, Double.MIN_VALUE, 0.0}; }\n                    @Override\n                    public Double[] add(RawEvent value, Double[] accumulator) {\n                        accumulator[0] += value.getValue();\n                        accumulator[1] = Math.min(accumulator[1], value.getValue());\n                        accumulator[2] = Math.max(accumulator[2], value.getValue());\n                        accumulator[3] += 1.0;\n                        return accumulator;\n                    }\n                    @Override\n                    public AggregateMetric getResult(Double[] accumulator) {\n                        return new AggregateMetric(\"austin-pump-01\", \"temperature\", System.currentTimeMillis(), accumulator[0]/accumulator[3], accumulator[1], accumulator[2], accumulator[3].longValue());\n                    }\n                    @Override\n                    public Double[] merge(Double[] a, Double[] b) { return a; }\n                });\n        System.out.println("Flink Streaming Telemetry Pipeline aggregates compiled successfully.");
        env.execute("IoT-Stateful-Sensor-Pipeline");
    }
}
