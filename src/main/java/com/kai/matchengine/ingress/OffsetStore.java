package com.kai.matchengine.ingress;

/** 消费起始 offset 来源（后续由快照实现替换）。 */
public interface OffsetStore {

  /**
   * 返回应 {@code seek} 的起始 offset（inclusive）。
   *
   * @param topic topic
   * @param partition 分区
   */
  long loadStartingOffset(String topic, int partition);
}
