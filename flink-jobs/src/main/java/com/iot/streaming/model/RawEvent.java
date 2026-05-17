package com.iot.streaming.model;

import java.io.Serializable;

public class RawEvent implements Serializable {
    private String eventId;
    private String assetId;
    private String deviceClass;
    private String metricName;
    private Double value;
    private Long eventTs;
    private Integer qualityFlag;

    public RawEvent() {}

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getAssetId() { return assetId; }
    public void setAssetId(String assetId) { this.assetId = assetId; }

    public String getDeviceClass() { return deviceClass; }
    public void setDeviceClass(String deviceClass) { this.deviceClass = deviceClass; }

    public String getMetricName() { return metricName; }
    public void setMetricName(String metricName) { this.metricName = metricName; }

    public Double getValue() { return value; }
    public void setValue(Double value) { this.value = value; }

    public Long getEventTs() { return eventTs; }
    public void setEventTs(Long eventTs) { this.eventTs = eventTs; }

    public Integer getQualityFlag() { return qualityFlag; }
    public void setQualityFlag(Integer qualityFlag) { this.qualityFlag = qualityFlag; }
}
// Version update 31\n
