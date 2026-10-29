package com.kai.matchengine.ingress.disruptor;

import com.kai.matchengine.event.inbound.MatchEvent;
import com.kai.matchengine.ingress.ValidatedInbound;

/** Disruptor 环上槽位（复用，处理后需 clear）。 */
public final class InboundRingEvent {

  private MatchEvent event;
  private int symbolId;
  private String topic;
  private int partition;
  private long offset;
  private boolean rejected;

  public void load(ValidatedInbound inbound) {
    this.event = inbound.event();
    this.symbolId = inbound.symbolId();
    this.topic = inbound.topic();
    this.partition = inbound.partition();
    this.offset = inbound.offset();
    this.rejected = false;
  }

  public void loadRejected(String topic, int partition, long offset) {
    this.event = null;
    this.symbolId = 0;
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.rejected = true;
  }

  public void clear() {
    event = null;
    topic = null;
    symbolId = 0;
    partition = 0;
    offset = -1L;
    rejected = false;
  }

  public boolean rejected() {
    return rejected;
  }

  public MatchEvent event() {
    return event;
  }

  public int symbolId() {
    return symbolId;
  }

  public String topic() {
    return topic;
  }

  public int partition() {
    return partition;
  }

  public long offset() {
    return offset;
  }
}
