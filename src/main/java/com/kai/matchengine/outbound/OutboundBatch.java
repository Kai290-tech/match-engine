package com.kai.matchengine.outbound;

import com.kai.matchengine.event.outbound.OutboundEvent;
import java.util.List;
import java.util.Objects;

/**
 * 一条 {@code match_event}（Kafka offset）产生的全部出站事件，视为一批。
 *
 * <p>整批全部 Kafka produce 成功后才能推进 {@link OutboundWatermark}。
 */
public record OutboundBatch(
    String matchTopic,
    int matchPartition,
    long matchOffset,
    int symbolId,
    List<OutboundEvent> events) {

  public OutboundBatch {
    Objects.requireNonNull(matchTopic, "matchTopic");
    Objects.requireNonNull(events, "events");
    events = List.copyOf(events);
  }
}
