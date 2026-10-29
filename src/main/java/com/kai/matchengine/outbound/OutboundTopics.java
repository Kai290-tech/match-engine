package com.kai.matchengine.outbound;

import java.util.Objects;

/** 出站 Kafka topic 名。 */
public record OutboundTopics(
    String eventResult, String trade, String orderUpdate, String bookUpdate) {

  public static final OutboundTopics DEFAULTS =
      new OutboundTopics("event_result", "trade", "order_update", "book_update");

  public OutboundTopics {
    Objects.requireNonNull(eventResult, "eventResult");
    Objects.requireNonNull(trade, "trade");
    Objects.requireNonNull(orderUpdate, "orderUpdate");
    Objects.requireNonNull(bookUpdate, "bookUpdate");
  }
}
