package com.iot.streaming.model;

import java.io.Serializable;

public class AggregateMetric implements Serializable {
    private String assetId;
    private String metricName;
    private Long windowEnd;
    private Double mean;
    private Double min;
    private Double max;
    private Long count;

    public AggregateMetric() {}

    public AggregateMetric(String assetId, String metricName, Long windowEnd, Double mean, Double min, Double max, Long count) {
        this.assetId = assetId;
        this.metricName = metricName;
        this.windowEnd = windowEnd;
        this.mean = mean;
        this.min = min;
        this.max = max;
        this.count = count;
    }

    public String getAssetId() { return assetId; }
    public String getMetricName() { return metricName; }
    public Long getWindowEnd() { return windowEnd; }
    public Double getMean() { return mean; }
    public Double getMin() { return min; }
    public Double getMax() { return max; }
    public Long getCount() { return count; }
}
