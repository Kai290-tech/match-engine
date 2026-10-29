package com.kai.matchengine.ingress;

/**
 * 非法/拒绝的 match_event 须与合法消息同样按 offset 顺序进入 Disruptor，由撮合线程推进空 outbound batch。
 */
@FunctionalInterface
public interface RejectedOffsetHandler {

  RejectedOffsetHandler NOOP = (topic, partition, offset) -> {};

  void onRejected(String topic, int partition, long offset);
}
