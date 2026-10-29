package com.kai.matchengine.outbound;

import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 按配置水位打 WARN 日志（节流）。 */
public final class LoggingBackpressureMonitor implements BackpressureMonitor {

  private static final Logger log = LoggerFactory.getLogger(LoggingBackpressureMonitor.class);

  private final double[] warnRatios;
  private final long logThrottleMs;
  private final boolean[] crossed;
  private long lastWarnMillis;

  public LoggingBackpressureMonitor(double[] warnRatios, long logThrottleMs) {
    if (warnRatios == null || warnRatios.length == 0) {
      this.warnRatios = new double[] {0.5, 0.75, 0.9};
    } else {
      this.warnRatios = Arrays.copyOf(warnRatios, warnRatios.length);
      Arrays.sort(this.warnRatios);
    }
    this.logThrottleMs = Math.max(1_000L, logThrottleMs);
    this.crossed = new boolean[this.warnRatios.length];
  }

  @Override
  public void onEnqueueDepth(int depth, int capacity, long pendingOffset, int symbolId) {
    if (capacity <= 0) {
      return;
    }
    double ratio = (double) depth / capacity;
    long now = System.currentTimeMillis();

    for (int i = 0; i < warnRatios.length; i++) {
      if (ratio >= warnRatios[i]) {
        if (!crossed[i] && now - lastWarnMillis >= logThrottleMs) {
          log.warn(
              "outbound queue high-water depth={}/{} ratio={} pendingOffset={} symbolId={}",
              depth,
              capacity,
              String.format("%.2f", ratio),
              pendingOffset,
              symbolId);
          lastWarnMillis = now;
        }
        crossed[i] = true;
      } else {
        crossed[i] = false;
      }
    }

    if (depth >= capacity) {
      if (now - lastWarnMillis >= logThrottleMs) {
        log.warn(
            "outbound backpressure blocked pendingOffset={} symbolId={} depth={}/{}",
            pendingOffset,
            symbolId,
            depth,
            capacity);
        lastWarnMillis = now;
      }
    }
  }
}
