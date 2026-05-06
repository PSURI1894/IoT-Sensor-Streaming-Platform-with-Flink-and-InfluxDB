package com.iot.streaming.model;

import java.io.Serializable;

public class EnrichedEvent implements Serializable {
    private String eventId;
    private String assetId;
    private String deviceClass;
    private String metricName;
    private Double value;
    private Long eventTs;
    private Integer qualityFlag;
    
    // Enriched variables from relational CDC temporal join
    private String siteName;
    private String siteLocation;
    private Double tempThreshold;
    private Double vibrationThreshold;
    private Double calibrationConstant;

    public EnrichedEvent() {}
    
    public EnrichedEvent(RawEvent raw, DeviceRegistryRecord reg) {
        this.eventId = raw.getEventId();
        this.assetId = raw.getAssetId();
        this.deviceClass = raw.getDeviceClass();
        this.metricName = raw.getMetricName();
        this.qualityFlag = raw.getQualityFlag();
        
        // Calibration multiplication constant
        if (reg != null) {
            this.value = raw.getValue() * reg.getCalibrationConstant();
            this.siteName = reg.getSiteName();
            this.siteLocation = reg.getSiteLocation();
            this.tempThreshold = reg.getTempThreshold();
            this.vibrationThreshold = reg.getVibrationThreshold();
            this.calibrationConstant = reg.getCalibrationConstant();
        } else {
            this.value = raw.getValue();
            this.tempThreshold = 85.0;
            this.vibrationThreshold = 4.5;
            this.calibrationConstant = 1.0;
        }
        this.eventTs = raw.getEventTs();
    }

    public String getEventId() { return eventId; }
    public String getAssetId() { return assetId; }
    public String getDeviceClass() { return deviceClass; }
    public String getMetricName() { return metricName; }
    public Double getValue() { return value; }
    public Long getEventTs() { return eventTs; }
    public Integer getQualityFlag() { return qualityFlag; }
    public String getSiteName() { return siteName; }
    public String getSiteLocation() { return siteLocation; }
    public Double getTempThreshold() { return tempThreshold; }
    public Double getVibrationThreshold() { return vibrationThreshold; }
    public Double getCalibrationConstant() { return calibrationConstant; }
}
