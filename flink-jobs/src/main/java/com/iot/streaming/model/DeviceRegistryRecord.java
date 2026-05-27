package com.iot.streaming.model;

import java.io.Serializable;

public class DeviceRegistryRecord implements Serializable {
    private String assetId;
    private String siteName;
    private String siteLocation;
    private Double tempThreshold;
    private Double vibrationThreshold;
    private Double calibrationConstant;

    public DeviceRegistryRecord() {}

    public String getAssetId() { return assetId; }
    public void setAssetId(String assetId) { this.assetId = assetId; }

    public String getSiteName() { return siteName; }
    public void setSiteName(String siteName) { this.siteName = siteName; }

    public String getSiteLocation() { return siteLocation; }
    public void setSiteLocation(String siteLocation) { this.siteLocation = siteLocation; }

    public Double getTempThreshold() { return tempThreshold; }
    public void setTempThreshold(Double tempThreshold) { this.tempThreshold = tempThreshold; }

    public Double getVibrationThreshold() { return vibrationThreshold; }
    public void setVibrationThreshold(Double vibrationThreshold) { this.vibrationThreshold = vibrationThreshold; }

    public Double getCalibrationConstant() { return calibrationConstant; }
    public void setCalibrationConstant(Double calibrationConstant) { this.calibrationConstant = calibrationConstant; }
}
