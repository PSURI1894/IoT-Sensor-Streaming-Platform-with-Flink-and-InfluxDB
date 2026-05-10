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
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))\n                .withTimestampAssigner((event, timestamp) -> {\n                    // Event-time safety clamp: bounds incoming times to ingestion_ts +/- 5 min\n                    long ingestionTs = System.currentTimeMillis();\n                    long eventTs = ingestionTs; // Parsing mock\n                    return Math.max(ingestionTs - 300000, Math.min(eventTs, ingestionTs + 300000));\n                }), 
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

        System.out.println("Flink Streaming Telemetry Pipeline bootstrapped successfully.");
        env.execute("IoT-Stateful-Sensor-Pipeline");
    }
}
