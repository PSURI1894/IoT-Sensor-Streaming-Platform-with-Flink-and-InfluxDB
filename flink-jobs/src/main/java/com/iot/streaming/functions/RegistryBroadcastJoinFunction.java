package com.iot.streaming.functions;

import com.iot.streaming.model.RawEvent;
import com.iot.streaming.model.DeviceRegistryRecord;
import com.iot.streaming.model.EnrichedEvent;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class RegistryBroadcastJoinFunction extends KeyedBroadcastProcessFunction<String, RawEvent, DeviceRegistryRecord, EnrichedEvent> {

    private final MapStateDescriptor<String, DeviceRegistryRecord> stateDescriptor =
            new MapStateDescriptor<>("registryState", String.class, DeviceRegistryRecord.class);

    @Override
    public void processElement(RawEvent raw, ReadOnlyContext ctx, Collector<EnrichedEvent> out) throws Exception {
        ReadOnlyBroadcastState<String, DeviceRegistryRecord> registryState = ctx.getBroadcastState(stateDescriptor);
        DeviceRegistryRecord record = registryState.get(raw.getAssetId());
        out.collect(new EnrichedEvent(raw, record));
    }

    @Override
    public void processBroadcastElement(DeviceRegistryRecord record, Context ctx, Collector<EnrichedEvent> out) throws Exception {
        ctx.getBroadcastState(stateDescriptor).put(record.getAssetId(), record);
    }
}
