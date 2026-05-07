package com.iot.streaming.model;

import java.io.Serializable;

public class AlertEvent implements Serializable {
    private String eventId;
    private String assetId;
    private String ruleId;
    private String metricName;
    private Double triggeredValue;
    private Double thresholdValue;
    private String severity;
    private Long timestamp;

    public AlertEvent() {}

    public AlertEvent(String eventId, String assetId, String ruleId, String metricName, Double triggeredValue, Double thresholdValue, String severity, Long timestamp) {
        this.eventId = eventId;
        this.assetId = assetId;
        this.ruleId = ruleId;
        this.metricName = metricName;
        this.triggeredValue = triggeredValue;
        this.thresholdValue = thresholdValue;
        this.severity = severity;
        this.timestamp = timestamp;
    }

    public String getEventId() { return eventId; }
    public String getAssetId() { return assetId; }
    public String getRuleId() { return ruleId; }
    public String getMetricName() { return metricName; }
    public Double getTriggeredValue() { return triggeredValue; }
    public Double getThresholdValue() { return thresholdValue; }
    public String getSeverity() { return severity; }
    public Long getTimestamp() { return timestamp; }
}
