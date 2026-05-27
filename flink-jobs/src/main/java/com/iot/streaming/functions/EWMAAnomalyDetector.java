package com.iot.streaming.functions;

import com.iot.streaming.model.EnrichedEvent;
import com.iot.streaming.model.AlertEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class EWMAAnomalyDetector extends KeyedProcessFunction<String, EnrichedEvent, AlertEvent> {

    private transient ValueState<Double> ewmaState;
    private transient ValueState<Double> varianceState;
    private static final double ALPHA = 0.05; // Smoothing average
    private static final double BETA = 0.10;  // Smoothing variance

    @Override
    public void open(Configuration parameters) throws Exception {
        ewmaState = getRuntimeContext().getState(new ValueStateDescriptor<>("ewma", TypeInformation.of(Double.class)));
        varianceState = getRuntimeContext().getState(new ValueStateDescriptor<>("variance", TypeInformation.of(Double.class)));
    }

    @Override
    public void processElement(EnrichedEvent event, Context ctx, Collector<AlertEvent> out) throws Exception {
        Double lastEwma = ewmaState.value();
        Double lastVariance = varianceState.value();
        double currentVal = event.getValue();

        if (lastEwma == null) {
            ewmaState.update(currentVal);
            varianceState.update(0.0);
            return;
        }

        // Calculate EWMA & Rolling Variance
        double nextEwma = ALPHA * currentVal + (1 - ALPHA) * lastEwma;
        double diff = currentVal - lastEwma;
        double nextVariance = (1 - BETA) * lastVariance + BETA * Math.pow(diff, 2);

        ewmaState.update(nextEwma);
        varianceState.update(nextVariance);

        double stdDev = Math.sqrt(nextVariance);
        
        // Threshold check: 3-sigma anomaly detection
        if (stdDev > 0.01 && Math.abs(currentVal - nextEwma) > 3.0 * stdDev) {
            out.collect(new AlertEvent(
                event.getEventId() + "_ewma_alert",
                event.getAssetId(),
                "EWMA_3SIGMA_BREACH",
                event.getMetricName(),
                currentVal,
                nextEwma + 3.0 * stdDev,
                "CRITICAL",
                event.getEventTs()
            ));
        }
    }
}
