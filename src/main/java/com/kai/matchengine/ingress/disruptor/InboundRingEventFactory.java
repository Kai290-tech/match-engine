package com.kai.matchengine.ingress.disruptor;

import com.lmax.disruptor.EventFactory;

public final class InboundRingEventFactory implements EventFactory<InboundRingEvent> {

  @Override
  public InboundRingEvent newInstance() {
    return new InboundRingEvent();
  }
}
