package com.iot.streaming.sinks;

import com.iot.streaming.model.EnrichedEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.util.HashMap;
import java.util.Map;

public class IcebergSinkBuilder {

    public static void build(DataStream<EnrichedEvent> stream, String warehousePath) {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "hadoop");
        properties.put("warehouse", warehousePath);

        CatalogLoader catalogLoader = CatalogLoader.hadoop("hadoop_catalog", new org.apache.hadoop.conf.Configuration(), properties);
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, org.apache.iceberg.catalog.TableIdentifier.of("db", "telemetry_raw"));

        FlinkSink.forRowData(stream.map(event -> {
            // RowData mapping logic converting POJO to Flink RowData for Parquet commit
            return org.apache.flink.table.data.GenericRowData.of(
                org.apache.flink.table.data.binary.BinaryStringData.fromString(event.getEventId()),
                org.apache.flink.table.data.binary.BinaryStringData.fromString(event.getAssetId()),
                event.getValue(),
                event.getEventTs()
            );
        }, org.apache.flink.api.common.typeinfo.TypeInformation.of(org.apache.flink.table.data.RowData.class)))
        .tableLoader(tableLoader)
        .writeParallelism(2)
        .append();
    }
}
