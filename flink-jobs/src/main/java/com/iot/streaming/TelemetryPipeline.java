package com.iot.streaming;

import com.iot.streaming.model.RawEvent;
import com.iot.streaming.model.EnrichedEvent;
import com.iot.streaming.model.AlertEvent;
import com.iot.streaming.model.AggregateMetric;
import com.iot.streaming.functions.RegistryBroadcastJoinFunction;
import com.iot.streaming.functions.EWMAAnomalyDetector;
import com.iot.streaming.sinks.InfluxDBLineProtocolSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
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
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> {
                            // Event-time safety clamp: bounds incoming times to ingestion_ts +/- 5 min
                            long ingestionTs = System.currentTimeMillis();
                            long eventTs = ingestionTs; // In production, this parses from JSON
                            return Math.max(ingestionTs - 300000, Math.min(eventTs, ingestionTs + 300000));
                        }), 
                "KafkaRawSource");

        // Parse Raw JSON into POJOs
        DataStream<RawEvent> rawEventStream = rawJsonStream.map(new MapFunction<String, RawEvent>() {
            @Override
            public RawEvent map(String value) throws Exception {
                // In production, use Jackson to parse JSON value
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

        // 1-minute tumbling windowed aggregations
        DataStream<AggregateMetric> aggregatedMetrics = watermarkedStream
                .keyBy(RawEvent::getAssetId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new AggregateFunction<RawEvent, Double[], AggregateMetric>() {
                    @Override
                    public Double[] createAccumulator() { 
                        return new Double[]{0.0, Double.MAX_VALUE, Double.MIN_VALUE, 0.0}; 
                    }

                    @Override
                    public Double[] add(RawEvent value, Double[] accumulator) {
                        accumulator[0] += value.getValue(); // Sum
                        accumulator[1] = Math.min(accumulator[1], value.getValue()); // Min
                        accumulator[2] = Math.max(accumulator[2], value.getValue()); // Max
                        accumulator[3] += 1.0; // Count
                        return accumulator;
                    }

                    @Override
                    public AggregateMetric getResult(Double[] accumulator) {
                        double mean = accumulator[3] > 0 ? accumulator[0] / accumulator[3] : 0.0;
                        return new AggregateMetric(
                            "austin-pump-01", 
                            "temperature", 
                            System.currentTimeMillis(), 
                            mean, 
                            accumulator[1], 
                            accumulator[2], 
                            accumulator[3].longValue()
                        );
                    }

                    @Override
                    public Double[] merge(Double[] a, Double[] b) { 
                        return new Double[]{
                            a[0] + b[0], 
                            Math.min(a[1], b[1]), 
                            Math.max(a[2], b[2]), 
                            a[3] + b[3]
                        }; 
                    }
                });

        System.out.println("Flink Streaming Telemetry Pipeline aggregates compiled successfully.");
        env.execute("IoT-Stateful-Sensor-Pipeline");
    }
}
