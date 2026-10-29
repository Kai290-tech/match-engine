package com.kai.matchengine.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "snapshot")
public class SnapshotProperties {

  private long everyNOffsets = 1000L;
  private long intervalMs = 600_000L;
  private int retainCount = 2;
  private long persistAwaitTimeoutMs = 120_000L;
  /** memory | jdbc */
  private String store = "memory";

  private long deltaTriggerOffsetSpan = 10_000L;
  private long deltaTriggerDeltaCount = 10_000L;
  private int deltaLogRingCapacity = 20_000;
  private int deltaLogSpillCapacity = 20_000;
  private double fullMaxBookFillRatio = 0.30;
  private int fullMinBatchChainBeforeAlert = 50;

  public long getDeltaTriggerOffsetSpan() {
    return deltaTriggerOffsetSpan;
  }

  public void setDeltaTriggerOffsetSpan(long deltaTriggerOffsetSpan) {
    this.deltaTriggerOffsetSpan = deltaTriggerOffsetSpan;
  }

  public long getDeltaTriggerDeltaCount() {
    return deltaTriggerDeltaCount;
  }

  public void setDeltaTriggerDeltaCount(long deltaTriggerDeltaCount) {
    this.deltaTriggerDeltaCount = deltaTriggerDeltaCount;
  }

  public int getDeltaLogRingCapacity() {
    return deltaLogRingCapacity;
  }

  public void setDeltaLogRingCapacity(int deltaLogRingCapacity) {
    this.deltaLogRingCapacity = deltaLogRingCapacity;
  }

  public int getDeltaLogSpillCapacity() {
    return deltaLogSpillCapacity;
  }

  public void setDeltaLogSpillCapacity(int deltaLogSpillCapacity) {
    this.deltaLogSpillCapacity = deltaLogSpillCapacity;
  }

  public double getFullMaxBookFillRatio() {
    return fullMaxBookFillRatio;
  }

  public void setFullMaxBookFillRatio(double fullMaxBookFillRatio) {
    this.fullMaxBookFillRatio = fullMaxBookFillRatio;
  }

  public int getFullMinBatchChainBeforeAlert() {
    return fullMinBatchChainBeforeAlert;
  }

  public void setFullMinBatchChainBeforeAlert(int fullMinBatchChainBeforeAlert) {
    this.fullMinBatchChainBeforeAlert = fullMinBatchChainBeforeAlert;
  }

  public long getEveryNOffsets() {
    return everyNOffsets;
  }

  public void setEveryNOffsets(long everyNOffsets) {
    this.everyNOffsets = everyNOffsets;
  }

  public long getIntervalMs() {
    return intervalMs;
  }

  public void setIntervalMs(long intervalMs) {
    this.intervalMs = intervalMs;
  }

  public int getRetainCount() {
    return retainCount;
  }

  public void setRetainCount(int retainCount) {
    this.retainCount = retainCount;
  }

  public long getPersistAwaitTimeoutMs() {
    return persistAwaitTimeoutMs;
  }

  public void setPersistAwaitTimeoutMs(long persistAwaitTimeoutMs) {
    this.persistAwaitTimeoutMs = persistAwaitTimeoutMs;
  }

  public String getStore() {
    return store;
  }

  public void setStore(String store) {
    this.store = store;
  }
}
