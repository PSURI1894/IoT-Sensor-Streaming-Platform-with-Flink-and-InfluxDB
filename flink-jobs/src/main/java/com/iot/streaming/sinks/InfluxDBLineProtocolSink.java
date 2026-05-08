package com.iot.streaming.sinks;

import com.iot.streaming.model.EnrichedEvent;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class InfluxDBLineProtocolSink extends RichSinkFunction<EnrichedEvent> {

    private transient InfluxDBClient client;
    private transient WriteApiBlocking writeApi;
    private final String url;
    private final String token;
    private final String org;
    private final String bucket;
    private transient List<Point> batch;
    private static final int BATCH_SIZE = 1000;
    
    // Tag allowlist to mitigate Cardinality Explosion
    private static final Set<String> ALLOWED_TAGS = Set.of("site_id", "asset_id", "metric_name", "device_class");

    public InfluxDBLineProtocolSink(String url, String token, String org, String bucket) {
        this.url = url;
        this.token = token;
        this.org = org;
        this.bucket = bucket;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        client = InfluxDBClientFactory.create(url, token.toCharArray(), org, bucket);
        writeApi = client.getWriteApiBlocking();
        batch = new ArrayList<>();
    }

    @Override
    public void invoke(EnrichedEvent event, Context context) throws Exception {
        Point point = Point.measurement("device_telemetry")
                .addTag("asset_id", event.getAssetId())
                .addTag("metric_name", event.getMetricName())
                .addTag("device_class", event.getDeviceClass())
                .addField("value", event.getValue())
                .addField("quality", event.getQualityFlag())
                .addField("site_location", event.getSiteLocation() != null ? event.getSiteLocation() : "UNKNOWN")
                .time(event.getEventTs(), WritePrecision.MS);

        batch.add(point);

        if (batch.size() >= BATCH_SIZE) {
            flush();
        }
    }

    private void flush() {
        if (!batch.isEmpty()) {
            writeApi.writePoints(batch);
            batch.clear();
        }
    }

    @Override
    public void close() throws Exception {
        flush();
        if (client != null) {
            client.close();
        }
    }
}
