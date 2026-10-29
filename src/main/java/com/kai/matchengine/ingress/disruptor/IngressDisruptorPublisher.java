package com.kai.matchengine.ingress.disruptor;

import com.kai.matchengine.ingress.ValidatedInbound;
import com.kai.matchengine.ingress.ValidatedInboundHandler;

/**
 * Kafka ingress → Disruptor：合法与拒绝 offset 均按 publish 顺序进入单消费者，保证出站 watermark 连续。
 */
public interface IngressDisruptorPublisher extends ValidatedInboundHandler {

  void onRejected(String topic, int partition, long offset);
}
