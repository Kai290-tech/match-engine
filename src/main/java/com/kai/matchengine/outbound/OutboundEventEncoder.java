package com.kai.matchengine.outbound;

import com.kai.matchengine.event.outbound.OutboundEvent;

/** 出站事件编码（SBE / JSON 等）。 */
public interface OutboundEventEncoder {

  String topicFor(OutboundEvent event, OutboundTopics topics);

  /** Kafka value bytes；key 仍由 {@link OutboundPublisher#kafkaKey} 决定。 */
  byte[] encode(OutboundBatch batch, OutboundEvent event);
}
