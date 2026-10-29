package com.kai.matchengine.outbound;

/** 出站队列水位与阻塞可观测性（一期：日志；后续可接 metrics/告警）。 */
public interface BackpressureMonitor {

  BackpressureMonitor NOOP = (depth, capacity, offset, symbolId) -> {};

  /**
   * 入队前采样队列深度。
   *
   * @param depth 当前 {@code queue.size()}
   * @param capacity 队列容量
   * @param pendingOffset 即将入队的 match offset（无则 -1）
   * @param symbolId 批次 symbolId（无则 0）
   */
  void onEnqueueDepth(int depth, int capacity, long pendingOffset, int symbolId);
}
